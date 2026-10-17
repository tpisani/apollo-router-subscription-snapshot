export const ordersSDL = `
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])

  # Entidade só para enriquecimento federado (catalog contribui label).
  type Order @key(fields: "id", resolvable: false) { id: ID! }

  enum Operation { UPSERT DELETE }
  enum FrameKind {
    "Substitui todo o estado local do escopo."
    SNAPSHOT
    "Eventos contíguos a partir do cursor anterior do cliente."
    CHANGES
  }

  "Estado versionado: pertence ao mesmo commit do evento/snapshot."
  type OrderState {
    id: ID!
    version: Int!
    status: String!
    "Campos de outros subgraphs, resolvidos na entrega (consistência eventual)."
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
  type FeedFrame {
    scopeId: ID!
    kind: FrameKind!
    "Posição do cliente depois de aplicar o frame. Reenvie em after ao reconectar."
    cursor: ID!
    "Presente somente em SNAPSHOT."
    orders: [OrderState!]
    changes: [Change!]!
  }

  type Query {
    ordersSnapshot(scopeId: ID!): Snapshot!
  }
  type Mutation {
    upsertOrder(scopeId: ID!, id: ID!, status: String!): Change!
    deleteOrder(scopeId: ID!, id: ID!): Change!
  }
  type Subscription {
    """
    Feed com retomada. Sem after: SNAPSHOT e depois CHANGES ao vivo.
    Com after: só o que falta (ou SNAPSHOT se o histórico não cobrir) e depois ao vivo.
    """
    orderFeed(scopeId: ID!, after: ID): FeedFrame!
  }
`;

export const catalogSDL = `
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])
  type Order @key(fields: "id") { id: ID!, label: String! }
  type Query { catalogHealth: String! }
`;
