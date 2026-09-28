package middleware

type Router interface {
	Middleware
	SendTo(topic string, msg Message) error
}
