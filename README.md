# Install 

   * rebar3 compile
   * make compile-deps
   
# Run
   * make run 
   * rebar3 shell
   * adb_app:kickoff(all).

    If you want, you can pass your desired console log level to the adb_app:kickoff function. 
    For example, adb_app:kickoff(all, debug).
    
    Available log levels:  debug, info, notice, warning, error, critical, alert, emergency.