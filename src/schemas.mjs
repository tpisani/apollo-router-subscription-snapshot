export const ordersSDL = `
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])
  type Order @key(fields: "id") { id: ID!, status: String!, version: Int! }
  # Value object: não é uma entidade federada. O conteúdo pertence ao mesmo snapshot/evento.
  type OrderState { id: ID!, status: String!, version: Int! }
  type Snapshot { scopeId: ID!, cursor: ID!, orders: [OrderState!]! }
  type Change { scopeId: ID!, cursor: ID!, operation: String!, order: OrderState! }
  type ChangePage { events: [Change!]!, nextCursor: ID!, hasMore: Boolean!, through: ID! }
  type LiveFrame { scopeId: ID!, kind: String!, cursor: ID!, change: Change }
  type Query {
    ordersSnapshot(scopeId: ID!, fresh: Boolean! = false): Snapshot!
    orderHead(scopeId: ID!): ID!
    orderChanges(scopeId: ID!, after: ID!, through: ID!, first: Int! = 100): ChangePage!
    order(id: ID!): Order
  }
  type Mutation {
    upsertOrder(scopeId: ID!, id: ID!, status: String!): Change!
    deleteOrder(scopeId: ID!, id: ID!): Change!
  }
  type Subscription { orderEvents(scopeId: ID!): LiveFrame! }
`;

export const catalogSDL = `
  extend schema @link(url: "https://specs.apollo.dev/federation/v2.4", import: ["@key"])
  type Order @key(fields: "id") { id: ID!, label: String! }
  type Query { catalogHealth: String! }
`;
