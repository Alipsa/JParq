Please create a command line interface for interacting with Jparq.
The CLI should be based on JLine and pass all text input to the Jparq engine for execution.
The CLI should support command history and basic line editing features.
The CLI should also support a few commands starting with a slash (/) for controlling the CLI itself, such as:
/exit to exit the application.
/connect <base directory path> to connect to a database.
/help to display help information about the available commands.
/close to close the current database connection.
/list to list all tables in the connected database.
/describe <table name> to describe the schema of a specific table.
/info to display information about the current database connection.

Put the cli related code in the package `se.alipsa.jparq.cli`.
