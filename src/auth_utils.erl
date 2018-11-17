-module(auth_utils).

% API functions
-export([
	get_hash_as_hex/2,
	get_hash_as_hex/1
]).

% calculates the md5 hash, and then converts the digest to hex (as list), in the expected format of a doc key.
get_hash_as_hex(Data) ->
	get_hash_as_hex(Data, md5).

% calculates the hash using the given algorithm, and then converts the digest to hex (as list), in the expected format of a doc key.
get_hash_as_hex(Data, Hash_algorithm) ->
binary_to_list(bin_to_hex:bin_to_hex(crypto:hash(Hash_algorithm, Data))).
