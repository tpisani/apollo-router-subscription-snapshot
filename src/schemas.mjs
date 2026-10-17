const link = 'extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])';

// Placar: subgraph próprio, estado pequeno, frames com o estado inteiro.
export const scoresSDL = `
  ${link}
  enum Side { HOME AWAY }
  enum MatchStatus { SCHEDULED LIVE HALF_TIME FULL_TIME }
  enum ScoreboardFrameKind { STATE }

  "seconds vale no instante updatedAt (epoch ms); com running, o cliente avança sozinho."
  type Clock { running: Boolean!, seconds: Int!, updatedAt: Float! }
  type ScoreEvent { type: String!, side: Side, minute: Int! }
  type Scoreboard { status: MatchStatus!, period: Int!, home: Int!, away: Int!, clock: Clock!, lastEvent: ScoreEvent }
  type ScoreboardSnapshot { matchId: ID!, version: Int!, state: Scoreboard! }
  type ScoreboardFrame {
    matchId: ID!
    kind: ScoreboardFrameKind!
    version: Int!
    "Epoch ms do commit; nulo nos frames periódicos."
    committedAt: Float
    state: Scoreboard!
  }

  type Query {
    "Um placar consistente em alguma versão >= minVersion. Cacheável."
    scoreboard(matchId: ID!, minVersion: Int! = 0): ScoreboardSnapshot!
  }
  type Mutation {
    startPeriod(matchId: ID!, period: Int!): ScoreboardSnapshot!
    endPeriod(matchId: ID!, final: Boolean! = false): ScoreboardSnapshot!
    scoreGoal(matchId: ID!, side: Side!): ScoreboardSnapshot!
  }
  type Subscription {
    "Estado inteiro a cada mudança e a cada segundo. Mesmas variáveis para todos: deduplicável."
    scoreboardUpdates(matchId: ID!): ScoreboardFrame!
  }
`;

// Estatísticas: subgraph próprio, estado maior, frames agrupados com só o que mudou.
export const statsSDL = `
  ${link}
  enum Side { HOME AWAY }
  enum StatType { TOUCH PASS SHOT SHOT_ON_TARGET CORNER FOUL }
  enum StatsFrameKind { DELTA STATE CHECKPOINT }

  type TeamStats { possession: Int!, touches: Int!, shots: Int!, shotsOnTarget: Int!, passes: Int!, corners: Int!, fouls: Int! }
  type PlayerStats { id: ID!, touches: Int!, passes: Int!, shots: Int! }
  type MatchStats { home: TeamStats!, away: TeamStats!, players: [PlayerStats!]! }
  type StatsSnapshot { matchId: ID!, version: Int!, state: MatchStats! }
  "Valor final de um campo, por exemplo path [\\"home\\", \\"shots\\"] ou [\\"players\\", \\"h7\\", \\"passes\\"]."
  type StatPatch { path: [String!]!, value: Int }
  type StatsFrame {
    matchId: ID!
    kind: StatsFrameKind!
    "DELTA: versão inicial da janela. Aplicável se from <= versão local < version."
    from: Int
    version: Int!
    "Epoch ms do commit mais antigo da janela."
    committedAt: Float
    "DELTA: valores finais dos campos alterados na janela."
    patch: [StatPatch!]
    "STATE: o estado inteiro, quando é menor que o patch (rajadas). Substitui tudo."
    state: MatchStats
  }
  input StatEventInput { side: Side!, playerId: ID!, type: StatType! }

  type Query {
    "Estatísticas consistentes em alguma versão >= minVersion. Cacheável."
    matchStats(matchId: ID!, minVersion: Int! = 0): StatsSnapshot!
  }
  type Mutation {
    "Ingestão do provedor (incrementos). Devolve a nova versão."
    recordStats(matchId: ID!, events: [StatEventInput!]!): Int!
  }
  type Subscription {
    "Uma janela de 500 ms por frame (patch ou estado, o menor) e checkpoint a cada segundo. Deduplicável."
    statsUpdates(matchId: ID!): StatsFrame!
  }
`;

// Dados estáticos: cache longo, fora dos canais ao vivo.
export const catalogSDL = `
  ${link}
  enum Side { HOME AWAY }
  type Team @key(fields: "id") { id: ID!, name: String!, shortName: String! }
  type Player { id: ID!, name: String!, number: Int!, side: Side! }
  type Match @key(fields: "id") { id: ID!, kickoff: String!, home: Team!, away: Team!, players: [Player!]! }
  type Query { match(id: ID!): Match, matches: [Match!]! }
`;
