-module(adb_mr).
-behaviour(gen_server).

-define(TaskResultsTable, resultsTable).
-define(ManagerTasksTable, managerTable).
-define(WorkerTasksTable, workerTable).
-define(TaskDone, task_done).

%
% Node Info record:
% 
%   Holds the state for every MapReduce node. It carries the following info:
%   -> status: The status of the node, that can only be the atoms 'idle' (the node may execute tasks) or 'busy' (the node is currently processing a task);
%   -> currentTask: The current task of the node. It's none if the node is not processing any task. It's a record of 'taskInfo' holding the task information;
%

-record(nodeInfo, {status, database, managementTask, workerTask, lastResult}).

-record(managementTask, {tasksDistributed, documentCount, modules, result}).
-record(workerTask, {manager, documentIndexList, modules, taskStartDate}).

-record(nodeControl, {node, documentIndexList, taskStartDate}).

-record(taskInformation, {id, manager, documentIndexList, database, modules}).
-record(taskResult, {id, node, result, taskCompletionDate}).

-record(taskModules, {main, dependencies}).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
	terminate/2,
	simpleCall/1,
	mapTest/1,
	task1/1,
	createIndexList/2
    % code_change/3
]).

-export([start/0, start_task/2, quit/0, gen_key/0]).

-import(adbtree, [get_header/1, read_doc/2, doc_count/1, doc_list/1]).
-import(adb_utils, [get_database_name/2, get_vnode_name/1]).
-import(jsone, [decode/2]).

%-import(adb_mr_tests, [map1/1]).

% Public functions:

% start(ServerName, Module, Args, Options) -> Result
%
% This function spawns a gen server process and 
% register it locally with ?MODULE.
% ?MODULE is a macro that returns the name of the current module
% 
% ServerName={local, kv_db_server} specify that the process will be
% registered locally as kv_db_server.
%
% Module=?MODULE specify the name of the callback module,
% which is basically the module name that will implement the gen_server
% callbacks.
%
% Args=[] is a list of arguments you may pass along to the process.
%
% Options=[] is a list of options you may pass to the gen server
start() ->
    gen_server:start({local, ?MODULE}, ?MODULE, [], []).

% gen_server callbacks

% Module:init(Args) -> Result
%
% This function is called when a gen server process is started.
%
% Args is the list of arguments passed from gen_server:start/3.
% Since we're not using it anywhere in the function implementation
% we're just ignoring it prefixing an underscore to the variable.
%
% It returns {ok, State} where is the internal state of the gen_server.
init(_Args) ->
	ets:new(?TaskResultsTable, [named_table]),
	ets:new(?ManagerTasksTable, [named_table]),
	ets:new(?WorkerTasksTable, [named_table]),
    State = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = none},
    {ok, State}.

% SYNCRONOUS call to verify the state of the node.
%
% Return type: A tuple returning the atom 'ok', the Status of the node ("busy" or "idle") and the database name, if the node is busy, or 'none' if the node is 'idle'.
handle_call({mr_ping}, _, State = #nodeInfo{status = Status, database = Database, lastResult = LastResult}) ->
	{reply, {ok, Status, Database, LastResult}, State};



% SYNCRONOUS call to start the whole MapReduce operation.
%
% This is a high order function. Three of the received arguments are functions:
%
% Database: The string type name of the database in which will be applied the operations.
% Map: The map function to be applied on each document on the selected database.
% Reduce: The function to be applied on the results of the map function.
% Merge: An optional function, which can be applied on the results of the reduce function.
%
% This call receives a task to be executed, find all MapReduce nodes of the architecture, split the work to be done by the nodes and send all the information needed by the nodes to execute the task received.
%
% The ETS table which will be used to store the results sent by the nodes is clear and prepared to receive the results of the current task.
%
% The state of the node is changed to 'busy', in order to not execute other tasks. This state is back do 'idle' when all nodes send the task result back to the manager node.

handle_call({mr_task, Database, Modules = #taskModules{main = MainModule, dependencies = Dependencies}}, _, State = #nodeInfo{status = Status}) ->
	log("Major task received."),
	log(erlang:system_time(millisecond)),
    case Status of
        idle ->
			log("Idle manager."),
        	Nodes = get_node_list(),
        	case Nodes of
        		[] ->
					log("No nodes avaiable."),
        			{reply, {error, "No nodes avaiable."}, State};
        		_ ->
					%log("Nodes avaiable."),
					reset_table(),
					load_module(Dependencies),
					load_module(MainModule),
					TaskKey = gen_key(),
					%log("Entry modules loaded."),
					Task = distribute_tasks(Database, Modules, Nodes, TaskKey),
					log("Tasks distributed."),
					Modules = Task#managementTask.modules,					
					ets:insert(?ManagerTasksTable, {TaskKey, Task}), 
		            NewState = #nodeInfo{status = busy, database = Database, managementTask = TaskKey, workerTask = none, lastResult = none},
		            {reply, {ok, TaskKey, "Task stated."}, NewState}
        	end;            
        busy ->
			log("Node manager busy."),
            {reply, {error, "Server busy, task not stated."}, State};
        _ ->
			log("Node manager unexpected state."),
        	NewState = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = State#nodeInfo.lastResult},
            {reply, {error, "Unexpected server state. Returning to Idle State."}, NewState}
	end.




handle_cast({mr_task, #taskInformation{id = Id, manager = Manager, documentIndexList = DocIndexList, database = Database, modules = Modules = #taskModules{main = MainModule, dependencies = Dependencies}}}, State = #nodeInfo{status = Status, lastResult = LastResult}) ->
	%log("Worker task."),
	case Status of
        idle ->
			%log("Worker idle."),
			load_module(Dependencies),
			load_module(MainModule),
			%log("Entry modules loaded on worker."),
			WorkerTask = #workerTask{manager = Manager, documentIndexList = DocIndexList, modules = Modules, taskStartDate = none},
			ets:insert(?WorkerTasksTable, {Id, WorkerTask}),
			NewState = #nodeInfo{status = busy, database = Database, managementTask = none, workerTask = Id, lastResult = LastResult},
			%log("Worker new state created."),
			%gen_server:cast(?MODULE, {mr_worker}),
			{noreply, NewState, 5};
        _ ->
			log("Worker not idle."),
        	{noreply, State, 5}
	end;
handle_cast({mr_worker}, #nodeInfo{database = Database, workerTask = Id}) ->
		try
			[{_, WorkerTask = #workerTask{manager = Manager, documentIndexList = DocIndexList, modules = #taskModules{main = MainModule = {ModuleName, _, _}, dependencies = Dependencies}}} | _] = ets:lookup(?WorkerTasksTable, Id),
			log("Worker start task."),
			% Code to be removed when receiving the right name from persistance
			VNodeName = get_vnode_name(1),
			RealDBName = get_database_name(Database, VNodeName),
			NameIndex = RealDBName++"Index.adb",
			%
			{ok, Fp} = file:open(NameIndex, [read, binary]),
			{Settings, _} = get_header(Fp),
			{ok, DocList} = doc_list(Database),
			log("Worker doc list retrieved."),
			log("Worker -- IndexList"),
			%log(DocIndexList),
			MapResults = adb_map(DocIndexList, ModuleName, DocList, Settings),
			log("Map executed."),
			%log(MapResults),
			ReduceResults = adb_reduce(MapResults, ModuleName),
			log("Reduce executed."),
			Result = #taskResult{node = node(), result = ReduceResults, taskCompletionDate = calendar:universal_time()},
			gen_server:cast({adb_mr, Manager}, {task_done, Result}),
			log("Worker result sent."),
			remove_module(MainModule),
			remove_module(Dependencies),
			log("Worker entry modules unloaded."),
			DoneWorkerTask = WorkerTask#workerTask{modules = ?TaskDone},
			ets:insert(?WorkerTasksTable, {Id, DoneWorkerTask}),
			NewState = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = Result},
			file:close(Fp),
			log("Worker exit."),
			{noreply, NewState}
		catch
			error:Error ->
				log("Worker error."),
				{error, Error}
		end;	

handle_cast({task_done, Result = #taskResult{node = Node}}, NodeInfo = #nodeInfo{managementTask = Id}) ->
	%log("Manager result received."),
	ets:insert(?TaskResultsTable, {Node, Result}),
	Results = get_table_results(ets:tab2list(?TaskResultsTable)),
	[ { _, ManagementTask = #managementTask{modules = #taskModules{main = {ModuleName, _, _}}, tasksDistributed = DistributedTasks}} | _ ] = ets:lookup(?ManagerTasksTable, Id),
	%log("Manager - Result inserted on table."),
	%log("Manager - Tasks Distributed.ets:insert(?TaskResultsTable, {Node, Result}),"),
	%log(DistributedTasks),
	NumOfTasks = length(DistributedTasks),
	case length(Results) of
		NumOfTasks ->
			%log("Manager - All nodes sent result."),
			FinalResult = mergeResults(fun ModuleName:merge/1, Results),
			FinalManagementTask = ManagementTask#managementTask{result = FinalResult, modules = ?TaskDone},
			ets:insert(?ManagerTasksTable, {Id, FinalManagementTask}),
			log("Manager - Results merged."),
			log(erlang:system_time(millisecond)),			
			NewState = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = FinalResult};
		_ ->
			log("Manager - Nodes need to send results."),
			NewState = NodeInfo
	end,
	{noreply, NewState};
	

handle_cast(_, State) ->
    {noreply, State}.

handle_info(timeout, State = #nodeInfo{workerTask = Id}) ->
	[{ _, WorkerTask = #workerTask{taskStartDate = TaskStartDate}} | _] = ets:lookup(?WorkerTasksTable, Id),
	log("Timeout."),
	case TaskStartDate of
		none ->
			log("Worker did not star task."),
			NewWorkerTask = WorkerTask#workerTask{taskStartDate = calendar:universal_time()},
			ets:insert(?WorkerTasksTable, {Id, NewWorkerTask}),
			%log("Worker state updated."),
			gen_server:cast(?MODULE, {mr_worker}),
			%log("Worker started task."),
			{noreply, State};
		_ ->
			{noreply, State}
	end;
handle_info(_, State) ->
    {noreply, State}.

% Code change not implemented, because it was not used.
% code_change(_, State, _) -> 
%     {ok, State}.

terminate(normal, _) ->
    ok.


% Private functions

start_task(Map, Reduce) ->
    gen_server:call(?MODULE, {Map, Reduce}).

quit() ->
    gen_server:call(?MODULE, terminate).


%
%
%
%
%
%	Functions related to the database.
%
%
%
%

get_document_count(Database) ->
	{ok, Count} = doc_count(Database),
	Count.

get_document(Index, DocList, Settings) ->
	%log("Worker -- Index"),
	%log(Index),
	%log("Worker -- DocList"),
	%log(DocList),
	log(lists:nth(Index, DocList)),
	{_, DocPos} = lists:nth(Index, DocList),
	log("Worker -- DocPos"),
	log(DocPos),
	{ok, Doc, _} = read_doc(DocPos, Settings),
	log("Worker -- Doc."),
	%log(Doc),
	% Parsing the document to proplist.
	Parsed = parse_doc(Doc),
	log("Worker -- Parsed document"),
	%log(Parsed),
	Parsed.

parse_doc(Doc) ->
	decode(Doc, [{object_format, proplist}]).

%
%
%
%
%
%	
%
%
%
%

get_node_list() ->
    FullList = nodes(),
    check_nodes(FullList).

check_nodes([]) -> 
	[];
check_nodes([Target | Tail]) ->
	try gen_server:call({adb_mr, Target}, {mr_ping}) of
		{ok, idle, _, _} ->
			[ Target | check_nodes(Tail)];
		_ ->
			check_nodes(Tail)
	catch
		_ ->
			check_nodes(Tail)
	end.



distribute_tasks(Database, Modules, Nodes, Id) ->
	%log("Manager -- distribute tasks.."),
	DocCount = get_document_count(Database),
	%log("Manager -- Docs counted."),
	{RegularAmount, RemainingAmount} = split_work(length(Nodes), DocCount),
	%log("Manager -- Work splited."),
	DistributedTasks = lists:flatten(create_tasks(Nodes, 1, RegularAmount, RemainingAmount)),
	%log("Manager -- Tasks created."),
	%log(length(DistributedTasks)),
	send_tasks(DistributedTasks, Database, Modules, Id),
	%log("Manager -- Tasks sent."),
    #managementTask{tasksDistributed = DistributedTasks, documentCount = DocCount, modules = Modules, result = none}.

create_tasks([Last | []], Index, _, Remaining) ->
    IndexList = createIndexList(Index, Index+Remaining-1),
    NodeTask = #nodeControl{node = Last, documentIndexList = IndexList, taskStartDate = calendar:universal_time()},
    [NodeTask];
create_tasks([Head | Tail], Index, Regular, Remaining) ->
    IndexList = lists:flatten(createIndexList(Index, Index+Regular-1)),
    NodeTask = #nodeControl{node = Head, documentIndexList = IndexList, taskStartDate = calendar:universal_time()},
    [NodeTask | create_tasks(Tail, Index+Regular, Regular, Remaining)].

createIndexList(Start, Start) ->
    [Start | []];
createIndexList(Start, End) ->
    [Start | createIndexList(Start+1, End)].

send_tasks([], _, _, _) ->
    ok;
send_tasks([Target | Tail], Database, Modules, Id) ->
	%log("Manager -- Send tasks before crash."),
	Task = #taskInformation{id = Id, manager = node(), documentIndexList = Target#nodeControl.documentIndexList, database = Database, modules = Modules},
	%log("Manager -- Task created."),
	gen_server:cast({?MODULE, Target#nodeControl.node}, {mr_task, Task}),
	%log("Manager -- Task sent."),
    send_tasks(Tail, Database, Modules, Id).

split_work(NodeAmount, TotalDocs) ->
	case NodeAmount of
		1 ->
			{TotalDocs, TotalDocs};
		_ ->
			Regular = round(math:ceil(TotalDocs/NodeAmount)),
			Remaining = TotalDocs - (Regular * (NodeAmount-1)),
			{Regular, Remaining}
	end.

load_module({Module, Filename, Binary}) ->
	{module, Module} = code:load_binary(Module, Filename, Binary),
	{ok, Module};
load_module([]) ->
		ok;
load_module([ {Module, Filename, Binary} | Tail]) ->
	{module, Module} = code:load_binary(Module, Filename, Binary),
	load_module(Tail),
	ok.

remove_module({Module, _, _}) ->
	code:purge(Module),
	code:delete(Module),
	ok;
remove_module([]) ->
		ok;
remove_module([ {Module, _, _} | Tail]) ->
	code:purge(Module),
	code:delete(Module),
	remove_module(Tail),
	ok.

get_table_results([]) ->
	[];
get_table_results([Head | Tail]) ->
	{_, #taskResult{result = Result}} = Head,
	[Result | get_table_results(Tail)].

mergeResults(Merge, Results) ->
	%log("Manager -- Starting merge."),
	%log(Merge),
	%log("Manager -- Results."),
	%log(Results),
	case Merge of
		none ->
			%log("Merge -- none"),
			Results;
		_ ->
			%log("Merge -- merge found."),
			Merge(Results)
	end.

gen_key() ->
	Time = erlang:system_time(nano_seconds),
	StringTime = integer_to_list(Time),
	UniformRandom = rand:uniform(10000),
	StringRandom = integer_to_list(UniformRandom),
	{Id, _} = string:to_integer(StringRandom++StringTime),
	Ctx = hashids:new([{salt, "It is not MapReduce salt"}, {min_hash_length, 10}, {default_alphabet, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"}]),
	hashids:encode(Ctx, Id).

reset_table() ->
	ets:delete(?TaskResultsTable),
	ets:new(?TaskResultsTable, [named_table]).

adb_map([], _, _, _) ->
	[];
adb_map([Index | Tail], Module, DocList, Settings) ->
	log("Worker -- Starting Map"),
	Doc = get_document(Index, DocList, Settings),
	%log("Worker -- Doc got."),
	%log(Module),
	try Module:map(Doc) of
		MapResult ->
			log("Map works"),
			[MapResult | adb_map(Tail, Module, DocList, Settings)]
	catch
		Error ->
			log("Map error."),
			throw({adb_map_error, Error, Index})
	end.
	
adb_reduce([], _) ->
	[];
adb_reduce(Entry, Module) ->
	log("Starting Reduce"),
	try Module:reduce(Entry) of
		ReduceResult ->
			log("Reduce works."),
			ReduceResult
	catch
		Error ->
			log("Reduce error."),
			throw({adb_reduce_error, Error, Entry})
	end.

log(Message) ->
	io:format("~p~n", [Message]).



simpleCall(Pid) ->
	Module = adb_mr_tests,
	{_Module, Binary, Filename} = code:get_object_code(Module),
	%rpc:call(Pid, code, load_binary, [Module, Filename, Binary]), 	
	{ok, Res} = gen_server:call({high, Pid}, {mr, {Module, Filename, Binary}}),
	io:format("Ok! ~p~n", [Res]).

mapTest(Json) ->
	Doc = decode(Json, [{object_format, proplist}]),
	adb_mr_tests:map(Doc).

task1(DBName) ->
	Module = adb_mr_tests,
	{_Module, Binary, Filename} = code:get_object_code(Module),
	MrModules = #taskModules{main = {Module, Filename, Binary}, dependencies = []},
	Res = gen_server:call(?MODULE, {mr_task, DBName, MrModules}),
	{ok, Res}.