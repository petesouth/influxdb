package influxql

type FloatMeanReducer struct {
	sum   float64
	count uint32
}

func NewFloatMeanReducer() *FloatMeanReducer {
	return &FloatMeanReducer{}
}

func (r *FloatMeanReducer) AggregateFloat(p *FloatPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * float64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

func (r *FloatMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      r.sum / float64(r.count),
		Aggregated: r.count,
	}}
}

type IntegerMeanReducer struct {
	sum   int64
	count uint32
}

func NewIntegerMeanReducer() *IntegerMeanReducer {
	return &IntegerMeanReducer{}
}

func (r *IntegerMeanReducer) AggregateInteger(p *IntegerPoint) {
	if p.Aggregated >= 2 {
		r.sum += p.Value * int64(p.Aggregated)
		r.count += p.Aggregated
	} else {
		r.sum += p.Value
		r.count++
	}
}

func (r *IntegerMeanReducer) Emit() []FloatPoint {
	return []FloatPoint{{
		Time:       ZeroTime,
		Value:      float64(r.sum) / float64(r.count),
		Aggregated: r.count,
	}}
}

type FloatMovingAverageReducer struct {
	pos    int
	sum    float64
	buf    []float64
	points []FloatPoint
}

func NewFloatMovingAverageReducer(n int) *FloatMovingAverageReducer {
	return &FloatMovingAverageReducer{
		buf: make([]float64, 0, n),
	}
}

func (r *FloatMovingAverageReducer) AggregateFloat(p *FloatPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.pos = (r.pos + 1) % cap(r.buf)

	if len(r.buf) == cap(r.buf) {
		r.points = append(r.points, FloatPoint{
			Value:      r.sum / float64(len(r.buf)),
			Time:       p.Time,
			Aggregated: uint32(len(r.buf)),
		})
	}
}

func (r *FloatMovingAverageReducer) Emit() []FloatPoint {
	return r.points
}

type IntegerMovingAverageReducer struct {
	pos    int
	sum    int64
	buf    []int64
	points []FloatPoint
}

func NewIntegerMovingAverageReducer(n int) *IntegerMovingAverageReducer {
	return &IntegerMovingAverageReducer{
		buf: make([]int64, 0, n),
	}
}

func (r *IntegerMovingAverageReducer) AggregateInteger(p *IntegerPoint) {
	if len(r.buf) != cap(r.buf) {
		r.buf = append(r.buf, p.Value)
	} else {
		r.sum -= r.buf[r.pos]
		r.buf[r.pos] = p.Value
	}
	r.sum += p.Value
	r.pos = (r.pos + 1) % cap(r.buf)

	if len(r.buf) == cap(r.buf) {
		r.points = append(r.points, FloatPoint{
			Value:      float64(r.sum) / float64(len(r.buf)),
			Time:       p.Time,
			Aggregated: uint32(len(r.buf)),
		})
	}
}

func (r *IntegerMovingAverageReducer) Emit() []FloatPoint {
	return r.points
}
