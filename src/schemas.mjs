export const ordersSDL = `
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])

  # Entidade só para enriquecimento federado (catalog contribui label).
  type Order @key(fields: "id", resolvable: false) { id: ID! }

  enum Operation { UPSERT DELETE }
  enum FrameKind { CHANGE CHECKPOINT }

  "Estado versionado: pertence ao mesmo commit do evento/snapshot."
  type OrderState {
    id: ID!
    version: Int!
    status: String!
    "Campos de outros subgraphs (consistência eventual, cache próprio)."
    details: Order!
  }
  type Change {
    scopeId: ID!
    cursor: ID!
    operation: Operation!
    order: OrderState!
    "Epoch ms do commit, para medir latência ponta a ponta."
    committedAt: Float!
  }
  type Snapshot { scopeId: ID!, cursor: ID!, orders: [OrderState!]! }
  type ChangePage { scopeId: ID!, after: ID!, through: ID!, events: [Change!]! }
  type LiveFrame { scopeId: ID!, kind: FrameKind!, cursor: ID!, change: Change }

  type Query {
    """
    Um estado consistente em algum cursor >= minCursor. Qualquer resposta
    continua verdadeira para sempre, então pode vir de cache. minCursor = "0" é
    a chave compartilhada (pode estar antiga); um minCursor vindo do stream
    força uma base recente com uma chave nova.
    """
    ordersSnapshot(scopeId: ID!, minCursor: ID! = "0"): Snapshot!
    """
    Eventos (after, through], com through <= head e no máximo 500 eventos.
    Intervalo fechado: imutável e cacheável indefinidamente.
    """
    orderChanges(scopeId: ID!, after: ID!, through: ID!): ChangePage!
  }
  type Mutation {
    upsertOrder(scopeId: ID!, id: ID!, status: String!): Change!
    deleteOrder(scopeId: ID!, id: ID!): Change!
  }
  type Subscription {
    "Stream compartilhado do escopo, sem argumentos por cliente (deduplicável)."
    orderEvents(scopeId: ID!): LiveFrame!
  }
`;

export const catalogSDL = `
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])
  type Order @key(fields: "id") { id: ID!, label: String! }
  type Query { catalogHealth: String! }
`;
