@0xacbcfffefc7adcb4;

struct DateTime {
	unixTimestamp @0 :Int64;
	nanosecond @1 :UInt32;
}

struct RawDataPoint {
	location @0 :Text;
	path @1 :Text;
	component @2 :Text;
	timestamp @3 :DateTime;
	value :union {
		integer @4 :Int64;
		float @5 :Float64;
		boolean @6 :Bool;
		text @7 :Text;
	}
}
