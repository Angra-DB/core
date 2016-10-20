compile:
	erlc -DNOTEST -o ebin src/*.erl

tests:
	erlc -o ebin src/*.erl test/*.erl

deps:
	cd _build/default/lib/ebloom && ./rebar compile && cd -

run:
	ERL_FLAGS="-config app.config" $(REBAR) 
