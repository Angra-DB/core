compile:
	erlc -DNOTEST -o ebin src/*.erl

tests:
	erlc -o ebin src/*.erl test/*.erl
