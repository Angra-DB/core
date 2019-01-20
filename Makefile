# If the first argument is "run"...
ifeq (run,$(firstword $(MAKECMDGOALS)))
  # use the rest as arguments for "run"
  RUN_ARGS := $(wordlist 2,$(words $(MAKECMDGOALS)),$(MAKECMDGOALS))
  # ...and turn them into do-nothing targets
  $(eval $(RUN_ARGS):;@:)
endif

install:
	erlc -DNOTEST -o ebin src/*.erl

tests:
	erlc -o ebin src/*.erl test/*.erl

compile-deps:
	cd _build/default/lib/ebloom && ./rebar compile && cd -

run:
	ERL_FLAGS="-setcookie teste -config app.config" $(RUN_ARGS)
