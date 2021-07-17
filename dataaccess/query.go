package dataaccess

import "time"

// Query represents data filtering according the price bar's date field.
type Query struct {
	year      int
	month     time.Month
	day       int
	isNotLast bool
}

// Day gets the filter day value.
func (q Query) Day() int {
	return q.day
}

// IsLast gets if it is filter the last price bar only.
func (q Query) IsLast() bool {
	return !q.isNotLast
}

// Month gets the filter month value.
func (q Query) Month() time.Month {
	return q.month
}

// Year gets the filter year value.
func (q Query) Year() int {
	return q.year
}

// GT defines a query for selecting all the price bars that with a date greater than the specified date.
func (q *Query) GT(yy int, mm time.Month, dd int) {
	t := time.Date(yy, mm, dd, 0, 0, 0, 0, time.UTC)
	q.year = t.Year()
	q.month = t.Month()
	q.day = t.Day()
	q.isNotLast = true
}

// Last defines a query for selecting the last price bar.
func (q *Query) Last() {
	q.year = 0
	q.month = time.January
	q.day = 0
	q.isNotLast = false
}
