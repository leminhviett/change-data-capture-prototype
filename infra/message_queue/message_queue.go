package message_queue

type Message struct {
	TableName string
	Action    string
	NewValue  []byte
	OldValue  []byte
}
