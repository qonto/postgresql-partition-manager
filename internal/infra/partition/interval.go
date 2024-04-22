package partition

type (
	Interval string
)

const (
	DailyInterval   Interval = "daily"
	WeeklyInterval  Interval = "weekly"
	MonthlyInterval Interval = "monthly"
	YearlyInterval  Interval = "yearly"
)
