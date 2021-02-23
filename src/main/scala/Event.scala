case class Event(actor: Actor,
                 created_at: String,
                 id: BigInt,
                 org: String,
                 payload: Payload,
                 public: Boolean,
                 repo: String,
                 `type`: String)
