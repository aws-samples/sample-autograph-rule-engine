# IDMS Case Maker Agent — End-to-End Design

## Overview

The IDMS Case Maker Agent builds investigation cases when integrity signals fire. It follows the violation recipes defined in the *Integrity Violation Landscape* document (violation types A1–E4) to gather evidence, generates a narrative, and delivers a structured case to investigators.

---

## End-to-End Flow

### Step 1: Game Happens

NBA game is played. NBA's existing systems run their statistical models and produce signal flags across 21 production signals (as defined in the *IDMS Existing Signal Definitions* document).

**Result:** Flags land in NBA's source systems (SQL Server and Snowflake) across six signal families:
- Signals 1.1–1.7: Referee Game Signals (cover and over/under markets)
- Signals 2.1–2.5: Player Game Signals (participation, involvement, activity, judgment, execution)
- Signals 3.1–3.3: Outcome Trend Signals (referee/team, player prop, referee/player streaks & records)
- Signal 4.1: Integrity Play Signals (high-leverage plays)
- Signal 5.1: Game Line Signals (significant line movements)
- Signals 6.1–6.4: Player Prop Market Signals (parlays, market extremity, payout, account sizing)

---

### Step 2: Ingestion Pipeline

**Trigger:** Scheduled after game (or push from NBA systems).

Ingestion populates the graph with game data and signal events. Briefly:

- **Every game:** Game node, Player/Referee → PARTICIPATED_IN → Game edges, Market nodes (prop lines, spreads for this game)
- **When signals fire:** SignalEvent nodes with edges to subject, game, market, signal type. For betting signals (6.X): Betting Account nodes linked to the signal.
- **On completion:** Emits EventBridge event for each fired signal to trigger the agent.

*(Ingestion pipeline design will be documented separately.)*

---

### Step 3: Agent Triggered

**Trigger:** EventBridge event "signal_ingested" → Lambda → calls AgentCore `InvokeHarness`

**Input to agent:**
```json
{
  "signal_type": "2.2",
  "subject_id": "player_abc",
  "subject_type": "player",
  "game_id": "game_20260115_LAL_BOS",
  "risk_score": 2,
  "p_value": 0.002,
  "market": "PTS",
  "outcome_consistent": true
}
```

---

### Step 4: Agent Executes

The agent has the *Integrity Violation Landscape* document (A1–E4) in its system prompt. This document defines each violation type's primary signals, contributing context (supplemental signals + contextual data), and follow-up analysis questions. The agent knows that Signal 2.2 is the primary signal for violation A2.

**Agent call sequence:**

```
1. get_signals(subject=player_abc, game=game_001, family="2.X", market="PTS")
   → "Any other player game signals for same player/game/market?"

2. get_signals(subject=player_abc, game=game_001, family="6.X")
   → "Any betting signals for same player/game?"

3. get_signals(game=game_001, family="5.1")
   → "Any significant line movements for this game?"

4. get_signals(subject=player_abc, family="3.2")
   → "Any player prop streak signals?"

5. get_signals(subject=player_abc, family="2.2", time_window="30d")
   → "Trend of same signal in proximate games?"

6. get_signals(subject=player_abc, family="2.X", market="PTS", time_window="30d")
   → "Trend of Tier 2 flags in this market recently?"

7. get_context(subject=player_abc, game=game_001, type="box_score")
   → "What were the box score results vs prop lines?"

8. get_context(subject=player_abc, game=game_001, type="injury")
   → "Any pre-existing injury?"

9. get_context(subject=player_abc, game=game_001, type="game_summary")
   → "Blowout? OT? Close game?"

10. get_prior_cases(subject=player_abc)
    → "Any prior cases for this subject?"

11. get_prior_reviews(subject=player_abc)
    → [PLACEHOLDER: returns empty in Phase 1]

12. assign_severity(signals_found, context_found, prior_reviews)
    → [PLACEHOLDER: returns null in Phase 1]
```

---

### Step 5: Agent Generates Narrative

Using all gathered data, the LLM produces a natural language summary:

```
"Player ABC showed significantly reduced on-ball involvement in the Jan 15 game 
against BOS (on-ball 31% vs season average 44%, p=0.002). The player's PTS result 
(12) was well below the pre-game prop line of 22.5. 

Betting signals are directionally consistent: Market Extremity (6.2) flagged extreme 
under-side imbalance, and Account Bet Sizing (6.4) identified a concentrated account 
on unders. A 4-game under streak exists for this player's PTS prop (3.2).

No pre-existing injury was reported. Game was close (final margin 4), ruling out 
blowout-related benching. No OT.

This player has 1 prior open case (Case #891, violation A3, from Dec 2025)."
```

---

### Step 6: Agent Writes Case

```
Agent calls: write_case(
  subject_id: player_abc,
  game_id: game_20260115_LAL_BOS,
  violation_type: "A2",
  severity: null,  // placeholder — investigator assigns
  primary_signal: { signal_id, type: "2.2", risk: 2, p: 0.002, outcome_consistent: true },
  contributing_signals: [
    { signal_id, type: "6.2", flagged: true },
    { signal_id, type: "6.4", flagged: true },
    { signal_id, type: "3.2", flagged: true },
    { signal_id, type: "2.3", risk: 1, flagged: true }
  ],
  mitigating_context: [],
  additional_context: {
    box_score: "12 PTS vs 22.5 line",
    injury: null,
    game_summary: "close game, margin 4, no OT"
  },
  narrative: "...",
  follow_up_questions: [
    "Was a team dynamic (ball-dominant teammate returning) a factor in on-ball percentage changing?",
    "Did the opposing team's defensive scheme specifically target or deny this player?",
    "Was the player visibly limited physically in a way that would naturally reduce aggressiveness?"
  ]
)
```

**write_case creates in Neptune:**
```
[Case #N] → HAS_ALLEGATION → [Allegation]
[Allegation] → ABOUT → [Player ABC]
[Allegation] → IN_GAME → [Game 20260115_LAL_BOS]
[Allegation] → OF_VIOLATION → [ViolationType A2]
[Allegation] → EVIDENCED_BY → [SignalEvent 2.2 (current)]
[Allegation] → EVIDENCED_BY → [SignalEvent 6.2 (current)]
[Allegation] → EVIDENCED_BY → [SignalEvent 6.4 (current)]
[Allegation] → EVIDENCED_BY → [SignalEvent 3.2 (trend)]
[Case #N] → HAS_CONTEXT → [AdditionalContext node]
```

**write_case creates in DynamoDB:**
```json
{ "case_id": "N", "status": "open", "severity": null, "created": "2026-01-16T02:00:00Z" }
```

---

### Step 7: Investigator Notified

SNS/SES sends case to investigator queue with the narrative + follow-up questions.

---

### Step 8: Investigator Reviews (External)

Investigator reviews the case, assigns severity, writes verdict.

Review is written to Neptune (via API):
```
[Review] → REVIEWS → [Case #N]
[Reviewer Casey] → AUTHORED → [Review]
Review properties: verdict="confirmed", severity="HIGH", notes="...", date=...
```

---

### Step 9: Future Cases Use Reviews (Phase 2)

Next time Player ABC fires a signal:
- `get_prior_cases` returns Case #N
- `get_prior_reviews` returns the review (verdict=confirmed, severity=HIGH)
- Agent incorporates: "Prior confirmed case for this subject (HIGH). Increases concern."
- `assign_severity` uses review patterns to suggest severity

---

## Tools Detail

### Tool 1: `get_signals`

| | |
|--|--|
| **Source** | Neptune |
| **Input** | subject_id (opt), game_id (opt), signal_family (opt), market (opt), time_window (opt) |
| **Output** | List of SignalEvent objects |
| **Implementation** | Lambda traverses Neptune from subject/game nodes to connected SignalEvent nodes, filters by family/market/time |

### Tool 2: `get_context`

| | |
|--|--|
| **Source** | Snowflake |
| **Input** | subject_id, game_id, context_type ("box_score" / "injury" / "game_summary" / "betting_detail" / "line_movement") |
| **Output** | Structured contextual data |
| **Implementation** | Lambda queries appropriate Snowflake table based on context_type, returns formatted result |

### Tool 3: `get_prior_cases`

| | |
|--|--|
| **Source** | Neptune |
| **Input** | subject_id |
| **Output** | List of past cases (case_id, date, violation_type, status, severity) |
| **Implementation** | Lambda traverses from subject → Allegation → Case nodes |

### Tool 4: `get_prior_reviews` [PLACEHOLDER]

| | |
|--|--|
| **Source** | Neptune |
| **Input** | subject_id, violation_type (optional) |
| **Output** | Phase 1: empty list. Phase 2: list of reviews (verdict, severity, notes, date) |
| **Implementation** | Lambda traverses from subject → Allegation → Case → Review nodes |
| **Phase 2 Use** | Agent reads reviewer verdicts and incorporates reasoning: "similar pattern was confirmed before" or "dismissed because of injury — injury present again" |

### Tool 5: `assign_severity` [PLACEHOLDER]

| | |
|--|--|
| **Source** | None (agent-internal reasoning) |
| **Input** | primary_signal, contributing_signals, mitigating_context, prior_reviews |
| **Output** | Phase 1: null. Phase 2: severity (HIGH/MEDIUM/LOW) + reasoning |
| **Implementation** | Phase 1: returns null. Phase 2: agent applies rubric (TBD with NBA) informed by accumulated review patterns |

### Tool 6: `write_case`

| | |
|--|--|
| **Source** | Neptune + DynamoDB |
| **Input** | Full case object (subject, game, violation_type, signals, context, narrative, follow_up_questions, severity) |
| **Output** | case_id |
| **Implementation** | Lambda creates Case + Allegation + edges in Neptune, writes metadata to DynamoDB, optionally triggers SNS |

---

## Infrastructure

| Service | Role |
|---------|------|
| AgentCore Harness | Managed agent (model + system prompt + tool orchestration) |
| AgentCore Gateway | Lambda tools exposed as MCP-compatible endpoints |
| Lambda (ingestion) | Snowflake → Neptune (signals + game + participation) |
| Lambda (tools) | 6 functions behind Gateway querying Neptune or Snowflake |
| Neptune | Graph (entities, signals, cases, reviews, participation) |
| Snowflake | NBA's source data (box scores, injury, betting detail) |
| EventBridge | Signal event routing (triggers agent) |
| DynamoDB | Case operational state |
| SNS | Investigator notifications |

---

## Open Questions for NBA

1. What triggers the ingestion — do you push signals to us, or do we poll your tables?
2. Should every flagged signal create a case, or is there a minimum threshold (e.g. risk_score >= 2)?
3. What does a review contain? (verdict taxonomy, structured fields, free text?)
4. Do you want the system to assign severity in Phase 2, or is severity always human-assigned?
5. If automated severity, what criteria define HIGH vs MEDIUM vs LOW?
6. Should a prior confirmed case for the same subject automatically elevate future cases?
