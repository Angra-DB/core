-module(adb_mr).
-behaviour(gen_server).

-define(ManagerTable, mTable).

%
% Node Info record:
% 
%   Holds the state for every MapReduce node. It carries the following info:
%   -> status: The status of the node, that can only be the atoms 'idle' (the node may execute tasks) or 'busy' (the node is currently processing a task);
%   -> currentTask: The current task of the node. It's none if the node is not processing any task. It's a record of 'taskInfo' holding the task information;
%

-record(nodeInfo, {status, database, managementTask, workerTask, lastResult}).

-record(managementTask, {tasksDistributed, documentCount, modules, resultList, result}).
-record(workerTask, {manager, documentIndexList, modules, taskStartDate}).

-record(nodeControl, {node, documentIndexList, taskStartDate}).

-record(taskInformation, {manager, documentIndexList, database, modules}).
-record(taskResult, {node, result, taskCompletionDate}).

-record(taskModules, {main, dependencies}).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
	terminate/2,
	simpleCall/1,
	mapTest/1
    % code_change/3
]).

-export([start/0, start_task/2, quit/0]).

-import(adbtree, [get_header/1, read_doc/2, doc_count/1, doc_list/1]).
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
	ets:new(?ManagerTable, [named_table]),
    State = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = none},
    {ok, State}.

% SYNCRONOUS call to verify the state of the node.
%
% Return type: A tuple returning the atom 'ok', the Status of the node ("busy" or "idle") and the database name, if the node is busy, or 'none' if the node is 'idle'.
handle_call({mr_ping}, _, State = #nodeInfo{status = Status, database = Database}) ->
	{reply, {ok, Status, Database}, State};



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
    case Status of
        idle ->
        	Nodes = get_node_list(),
        	case Nodes of
        		[] ->
        			{reply, {error, "No nodes avaiable."}, State};
        		_ ->
					reset_table(),
					load_module(Dependencies),
					load_module(MainModule),
        			Task = distribute_tasks(Database, Modules, Nodes),
        			Modules = Task#managementTask.modules, 
		            NewState = #nodeInfo{status = busy, database = Database, managementTask = Task, workerTask = none, lastResult = none},
		            {reply, {ok, "Task stated."}, NewState}
        	end;            
        busy ->
            {reply, {error, "Server busy, task not stated."}, State};
        _ ->
        	NewState = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = State#nodeInfo.lastResult},
            {reply, {error, "Unexpected server state. Returning to Idle State."}, NewState}
	end.




handle_cast({mr_task, #taskInformation{manager = Manager, documentIndexList = DocIndexList, database = Database, modules = Modules = #taskModules{main = MainModule, dependencies = Dependencies}}}, State = #nodeInfo{status = Status, lastResult = LastResult}) ->
	case Status of
        idle ->
			load_module(Dependencies),
			load_module(MainModule),
        	WorkerTask = #workerTask{manager = Manager, documentIndexList = DocIndexList, modules = Modules, taskStartDate = none},
			NewState = #nodeInfo{status = busy, database = Database, managementTask = none, workerTask = WorkerTask, lastResult = LastResult},
			%gen_server:cast(?MODULE, {mr_worker}),
			{noreply, NewState, 5};
        _ ->
        	{noreply, State, 5}
	end;
handle_cast({mr_worker}, #nodeInfo{database = Database, workerTask = #workerTask{manager = Manager, documentIndexList = DocIndexList, modules = #taskModules{main = MainModule = {ModuleName, _, _}, dependencies = Dependencies}}}) ->
		try
			NameIndex = Database++"Index.adb",
			{ok, Fp} = file:open(NameIndex, [read, binary]),
			{Settings, _} = get_header(Fp),
			DocList = doc_list(Database),
			MapResults = adb_map(Database, DocIndexList, ModuleName, DocList, Settings),
			ReduceResults = adb_reduce(MapResults, ModuleName),
			Result = #taskResult{node = node(), result = ReduceResults, taskCompletionDate = calendar:universal_time()},
			gen_server:cast({adb_mr, Manager}, {task_done, Result}),
			remove_module(MainModule),
			remove_module(Dependencies),
			NewState = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = Result},
			file:close(Fp),
			{ok, NewState}
		catch
			error:Error ->
				{error, Error}
		end;	

handle_cast({task_done, Result = #taskResult{node = Node}}, NodeInfo = #nodeInfo{managementTask = #managementTask{modules = #taskModules{main = {ModuleName, _, _}}}}) ->
	ets:insert(?ManagerTable, {Node, Result}),
	Results = ets:tab2list(?ManagerTable),
	NumOfTasks = length(NodeInfo#managementTask.tasksDistributed),
	case length(Results) of
		NumOfTasks ->
			FinalResult = mergeResults(fun ModuleName:reduce/1, Results),
			NewState = #nodeInfo{status = idle, database = none, managementTask = none, workerTask = none, lastResult = FinalResult};
		_ ->
			NewState = NodeInfo
	end,
	{noreply, NewState};
	

handle_cast(_, State) ->
    {noreply, State}.

handle_info(timeout, State = #nodeInfo{workerTask = WorkerTask = #workerTask{taskStartDate = TaskStartDate}}) ->
	case TaskStartDate of
		none ->
			NewWorkerTask = WorkerTask#workerTask{taskStartDate = calendar:universal_time()},
			NewState = State#nodeInfo{workerTask = NewWorkerTask},
			gen_server:cast(?MODULE, {mr_worker}),
			{noreply, NewState};
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
	DocPos = lists:nth(Index, DocList),
	{ok, Doc, {ver, _}} = read_doc(DocPos, Settings),
	% Parsing the document to proplist.
	decode(Doc, [{object_format, proplist}]).

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
		{ok, idle, _} ->
			[ Target | check_nodes(Tail)];
		_ ->
			check_nodes(Tail)
	catch
		_ ->
			check_nodes(Tail)
	end.



distribute_tasks(Database, Modules, Nodes) ->
    DocCount = get_document_count(Database),
    {RegularAmount, RemainingAmount} = split_work(length(Nodes), DocCount),
    DistributedTasks = create_tasks(Nodes, 1, RegularAmount, RemainingAmount),
    send_tasks(DistributedTasks, Database, Modules),
    #managementTask{tasksDistributed = DistributedTasks, documentCount = DocCount, modules = Modules, resultList = [], result = none}.

create_tasks([Last | []], Index, _, Remaining) ->
    IndexList = createIndexList(Index, Index+Remaining),
    NodeTask = #nodeControl{node = Last, documentIndexList = IndexList, taskStartDate = calendar:universal_time()},
    NodeTask;
create_tasks([Head | Tail], Index, Regular, Remaining) ->
    IndexList = createIndexList(Index, Index+Regular),
    NodeTask = #nodeControl{node = Head, documentIndexList = IndexList, taskStartDate = calendar:universal_time()},
    [NodeTask | create_tasks(Tail, Index+Regular, Regular, Remaining)].

createIndexList(Start, Start) ->
    Start;
createIndexList(Start, End) ->
    [Start | createIndexList(Start+1, End)].

send_tasks([], _, _) ->
    ok;
send_tasks([Target | Tail], Database, Modules) ->
	Task = #taskInformation{manager = node(), documentIndexList = Target#nodeControl.documentIndexList, database = Database, modules = Modules},
	gen_server:cast({adb_mr, Target#nodeControl.node}, {mr_task, Task}),
    send_tasks(Tail, Database, Modules).

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

mergeResults(Merge, Results) ->
	case Merge of
		none ->
			Results;
		_ ->
			Merge(Results)
	end.


reset_table() ->
	ets:delete(?ManagerTable),
	ets:new(?ManagerTable, [named_table]).

adb_map(_, [], _, _, _) ->
	[];
adb_map(Database, [Index | Tail], Module, DocList, Settings) ->
	Doc = get_document(Index, DocList, Settings),
	try Module:map(Doc) of
		MapResult ->
			[MapResult | adb_map(Database, Tail, Module, DocList, Settings)]
	catch
		Error ->
			throw({adb_map_error, Error, Index})
	end.
	
adb_reduce([], _) ->
	[];
adb_reduce([Head | Tail], Module) ->
	try Module:reduce(Head) of
		ReduceResult ->
			[ReduceResult | adb_reduce(Tail, Module)]
	catch
		Error ->
			throw({adb_reduce_error, Error, Head})
	end.

simpleCall(Pid) ->
	Module = adb_mr_tests,
	{_Module, Binary, Filename} = code:get_object_code(Module),
	%rpc:call(Pid, code, load_binary, [Module, Filename, Binary]), 	
	{ok, Res} = gen_server:call({high, Pid}, {mr, {Module, Filename, Binary}}),
	io:format("Ok! ~p~n", [Res]).

mapTest(Json) ->
	Doc = decode(Json, [{object_format, proplist}]),
	adb_mr_tests:map(Doc).

create_task() ->
	Module = adb_mr_tests,
	{_Module, Binary, Filename} = code:get_object_code(Module),
	MrModules = #taskModules{main = {Module, Filename, Binary}, dependencies = []},
	gen_server:call(?MODULE, {mr_task, "Test", MrModules}),
	ok.