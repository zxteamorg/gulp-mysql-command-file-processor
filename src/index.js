'use strict';

var through = require('through2');
var gutil = require('gulp-util');
var mysql = require('mysql');

const PLUGIN_NAME = 'gulp-mysql-command-file-processor';

/**
 *
 * @param {string} _fileName - Name of the file being streamed
 * @param {array} _commandBuffer - Array of the processed commands
 * @param {nm$_mysql.dbConnect.client|dbConnect.client} _dbConnection - A live connection to the database
 * @param {integer} _verbosity - The log level required -- 0(NONE) - 3(Full)
 */
function processCommands(_fileName, _commandBuffer, _dbConnection, _verbosity, _force, cb) {
    var commandsDone = false;
    var commandCount = 0;
    var processNextCommand = true;
    var runCmd = function() {
        var msg = '';
        if (!commandsDone) {
            if (processNextCommand) {
                if (_verbosity > 1) {
                    msg = 'Executing \'' + _fileName + '\' query #' + (commandCount + 1) + ' ........ ';
                }

                if (_verbosity === 3) {
                    msg += _commandBuffer[commandCount];
                }

                if (msg) {
                    console.log(msg);
                }

                processNextCommand = false;
                const sqlCommand = _commandBuffer[commandCount];
                _dbConnection.query({sql: sqlCommand, timeout: 60000}, function(err) {
                    if (err) {
                        if (!_force) {
                            process.exit(-1);
                        } else {
                            console.log('Failed executed query #' + (commandCount + 1));
                            commandsDone = true;
                            cb(new gutil.PluginError(PLUGIN_NAME, "Cannot execute SQL command '" 
                                + sqlCommand + "'" + (_fileName != null ? " from the file '" + _fileName + "'" : "")
                                + ". Underlayer error: " + err + ""));
                            return;
                        }
                    } else {
                        if (_verbosity > 2) {
                            console.log('Successfully executed query #' + (commandCount + 1));
                        }

                        commandCount++;
                        if (commandCount === _commandBuffer.length) {
                            commandsDone = true;
                            if (_verbosity > 2) {
                                console.log('Executed ' + commandCount + ' commands from file \'' + _fileName + '\'');
                            }
                        } else {
                            processNextCommand = true;
                        }
                        setTimeout(runCmd, 40);
                    }
                });
            }
        }
        else {
            cb(); 
        }
    };

    if (_commandBuffer.length > 0) {
        runCmd();
    }
    else {
        cb(); 
    }
}

/**
 *
 * @param {string} _username - Database username
 * @param {string} _password - database user password
 * @param {string} _host - The database host server (defaults to localhost)
 * @param {string} _port - The port the host server is listening on (defaults to 3306)
 * @param {string} _verbosity - Log level DEFAULT Low -- 'NONE' - no logging; 'MED'|'M' - Medium logging; 'FULL@|'F' - Full logging
 * @param {string} _database - The database on the host server
 * @return {*|{hello}|{first, second}}
 */
function processCommandFile(_username, _password, _host, _port, _verbosity, _database, _force) {
    var buffer;
    var host = _host ? _host : 'localhost';
    var port = _port ? _port : 3306;
    var verbosity = _verbosity === 'FULL' || _verbosity === 'F' ? 3 : _verbosity === 'MED' || _verbosity === 'M' ? 2 : _verbosity === 'NONE' ? 0 : 1;
    var force = _force === false ? false : true;
    if (!(_username && _password)) {
        throw new gutil.PluginError(PLUGIN_NAME, 'Both database and username and password must be defined');
    }

    return through.obj(function(file, enc, cb) {
        if (file.isBuffer()) {
            buffer = file.contents;
        } else if (file.isStream()) {
            buffer = file.contents;
        } else {
            this.emit('error', new gutil.PluginError(PLUGIN_NAME, 'Buffers not supported!'));
            return cb();
        }

        var dataOffset = -1;
        var char;
        var commandBuffer = [];
        var command = '';
        var inString = false;
        var isEscaped = false;
        var isCommentBlock = 0; // 0 = false, 1 = begin, 2 = in block, 3 = end
        var delimiter = ';';
        var data = buffer.toString('utf8', 0, buffer.length);

        while (dataOffset < buffer.length) {
            char = data.charAt(dataOffset++);

            if (char === delimiter && !inString && !isEscaped && !isCommentBlock) {
                commandBuffer.push(command);
                command = '';
            } else {
                if (char === '\\') {
                    isEscaped = true;
                } else if (data.substr(dataOffset, 2) === '/*' && !inString && !isEscaped) {
                    isCommentBlock++;
                } else if (data.substr(dataOffset, 2) === '*/' && !inString && !isEscaped) {
                    isCommentBlock--;
                } else if (data.substr(dataOffset, 9).toLowerCase() === 'delimiter' && !inString && !isEscaped && !isCommentBlock) {
                    var nl = data.substr(dataOffset + 10).match('\r|\n').index;
                    delimiter = data.substr(dataOffset + 10, nl);
                    dataOffset += 10 + nl;
                } else if (!inString && !isEscaped && !isCommentBlock && (data.substr(dataOffset, 2) === '# ' || data.substr(dataOffset, 3) === '-- ')) {
                    var nl = data.substr(dataOffset).match('\r|\n').index;
                    dataOffset += nl; // skipping to the end of the line
                } else if (char === '\'' && !isEscaped) {
                    inString = !inString;
                }

                command += char;
            }

            if (isEscaped) {
                isEscaped = false;
            }
        }

        // ignoring new line at end of the buffer, but sending the last request even if it is not closed with `;`
        if (command.trim().length) {
            commandBuffer.push(command);
        }

        if (verbosity > 2) { console.log('Connecting to a database...'); }
        const dbConnection = mysql.createConnection({ host: host, user: _username, password: _password, port: port, database: _database });
        dbConnection.connect(function(err) {
            if (err) {
                cb(err);
                return;
            }
            if (verbosity > 2) { console.log('Connection to the database was established.'); }
            const name = file.path;
            if (verbosity > 0) {
                if (name && name != null) {
                    console.log('Processing \'' + name + '\'...');
                } else {
                    console.log('Processing an SQL script...');
                }
            }
            const self = this;
            processCommands(name, commandBuffer, dbConnection, verbosity, force, function(err) {
                if (verbosity > 2) { console.log('Close the database connection.'); }
                dbConnection.end(function() {});
                if(err) {
                    cb(err, file);
                } else {
                    cb(null, file);
                }
            });
        });
    });
}

module.exports = processCommandFile;
