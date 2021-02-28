import java.sql.Date

case class Event(actor: Actor,
                 created_at: Date,
                 id: String,
                 org: String,
                 payload: Payload,
                 publicField: Boolean,
                 repo: String,
                 `type`: String)