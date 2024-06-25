package partition

type (
	Interval string
)

const (
	Daily     Interval = "daily"
	Weekly    Interval = "weekly"
	Monthly   Interval = "monthly"
	Quarterly Interval = "quarterly"
	Yearly    Interval = "yearly"
)
