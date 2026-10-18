const link = 'extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])';

// Scoreboard: its own subgraph, small state, frames with the full state.
export const scoresSDL = `
  ${link}
  enum Side { HOME AWAY }
  enum MatchStatus { SCHEDULED LIVE HALF_TIME FULL_TIME }
  enum ScoreboardFrameKind { STATE }

  "seconds is the value at the instant updatedAt (epoch ms); while running, the client advances it on its own."
  type Clock { running: Boolean!, seconds: Int!, updatedAt: Float! }
  type ScoreEvent { type: String!, side: Side, minute: Int! }
  "kickoff: scheduled start (epoch ms). The client subscribes from kickoff − 15 min and stops at FULL_TIME."
  type Scoreboard { status: MatchStatus!, kickoff: Float, period: Int!, home: Int!, away: Int!, clock: Clock!, lastEvent: ScoreEvent }
  type ScoreboardSnapshot { matchId: ID!, version: Int!, state: Scoreboard! }
  type ScoreboardFrame {
    matchId: ID!
    kind: ScoreboardFrameKind!
    version: Int!
    "Epoch ms of the commit; null on periodic frames."
    committedAt: Float
    state: Scoreboard!
  }

  type Query {
    "A consistent scoreboard at some version >= minVersion. Cacheable."
    scoreboard(matchId: ID!, minVersion: Int! = 0): ScoreboardSnapshot!
  }
  type Mutation {
    startPeriod(matchId: ID!, period: Int!): ScoreboardSnapshot!
    endPeriod(matchId: ID!, final: Boolean! = false): ScoreboardSnapshot!
    scoreGoal(matchId: ID!, side: Side!): ScoreboardSnapshot!
    scheduleMatch(matchId: ID!, kickoff: Float!): ScoreboardSnapshot!
  }
  type Subscription {
    "Full state on every change and every second. Same variables for everyone: deduplicable. Ends after the FULL_TIME frame."
    scoreboardUpdates(matchId: ID!): ScoreboardFrame!
  }
`;

// Stats: its own subgraph, larger state, full-state frames limited to 2/s.
export const statsSDL = `
  ${link}
  enum Side { HOME AWAY }
  enum StatType { TOUCH PASS SHOT SHOT_ON_TARGET CORNER FOUL }
  enum StatsFrameKind { STATE }

  type TeamStats { possession: Int!, touches: Int!, shots: Int!, shotsOnTarget: Int!, passes: Int!, corners: Int!, fouls: Int! }
  type PlayerStats { id: ID!, touches: Int!, passes: Int!, shots: Int! }
  type MatchStats { home: TeamStats!, away: TeamStats!, players: [PlayerStats!]! }
  type StatsSnapshot { matchId: ID!, version: Int!, state: MatchStats! }
  type StatsFrame {
    matchId: ID!
    kind: StatsFrameKind!
    version: Int!
    "Epoch ms of the oldest commit included in the frame; null on periodic frames."
    committedAt: Float
    "The full state. The frame with the highest version replaces the previous ones."
    state: MatchStats!
  }
  input StatEventInput { side: Side!, playerId: ID!, type: StatType! }

  type Query {
    "Consistent stats at some version >= minVersion. Cacheable."
    matchStats(matchId: ID!, minVersion: Int! = 0): StatsSnapshot!
  }
  type Mutation {
    "Ingestion from the data provider (increments). Returns the new version."
    recordStats(matchId: ID!, events: [StatEventInput!]!): Int!
  }
  type Subscription {
    "Full state: right away when quiet, at most 1 every 500 ms in a burst, and every second. Deduplicable."
    statsUpdates(matchId: ID!): StatsFrame!
  }
`;

// Static data: long cache, outside the live channels.

export const catalogSDL = `
  ${link}
  enum Side { HOME AWAY }
  type Team @key(fields: "id") { id: ID!, name: String!, shortName: String! }
  type Player { id: ID!, name: String!, number: Int!, side: Side! }
  type Match @key(fields: "id") { id: ID!, kickoff: String!, home: Team!, away: Team!, players: [Player!]! }
  type Query { match(id: ID!): Match, matches: [Match!]! }
`;
