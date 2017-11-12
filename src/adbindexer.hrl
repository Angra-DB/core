-define(Version, "1.0").
-define(HashSize, 10000).
-define(SizeOfDocPos, 8).
-define(SizeOfDocKey, 14).
-define(SizeOfPosting, 22).
-define(SizeOfExtPosting, 38).
-define(SizeOfCount, 8).
-define(SizeOfWord, 60).
-define(SizeOfHeader, 5).
-define(SizeOfVersion, 5).
-define(SizeOfTerm, 72).
-define(SizeOfPointer, 8).
-define(Normal, 0).
-define(Extended, 1).

-record(posting, {docKey, docPos, docVersion}).
-record(posting_ext, {docKey, docPos, docVersion, fieldStart, fieldEnd}).
-record(token, {word, docPos}).
-record(token_ext, {word, docPos, fieldStart, fieldEnd}).
-record(term, {word, postings, normalPostings, extPostings, nextTerm}). % Implement these two counts in code.
-record(bucket, {terms, count}).
