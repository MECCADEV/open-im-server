package options

// Opts opts.
type Opts struct {
	Signal        *Signal
	IOSPushSound  string
	IOSBadgeCount bool
	Ex            string
	// Data contains non-sensitive, string-valued message identity metadata for
	// transports that support an application data payload (currently FCM).
	Data             map[string]string
	MentionedUserIDs []string
}

// Signal message id.
type Signal struct {
	ClientMsgID string
}
