namespace SimpleFluent;

class Logger
{

    const DEFAULT_TRANSPORT = "tcp";

    const PHP_STREAM_CLIENT_CONNECT = 4;
    const PHP_STREAM_CLIENT_PERSISTENT = 1;

    const BACKOFF_TYPE_EXPONENTIAL = 0x01;
    const BACKOFF_TYPE_USLEEP      = 0x02;

    protected defaultOptions = [
        "socket_timeout"     : 1, 
        "connection_timeout" : 1,
        "backoff_mode"       : 0x02,
        "backoff_base"       : 3,
        "usleep_wait"        : 1000,
        "persistent"         : false,
        "retry_socket"       : true,
        "max_write_retry"    : 3
    ];


    protected transport;
    protected options;
    protected socket;

    public function __construct(string host, int port = null, array options = [])
    {
        // 接続先設定
        let this->transport = self::getUri(host, port);

        // 接続オプション設定
        var k;
        for k in array_keys(this->defaultOptions) {
            if (array_key_exists(k, options)) {
                let this->options[k] = options[k];
            } else {
                let this->options[k] = this->defaultOptions[k];
            }
        }

    }

    public static function getUri(string hostString, int port)
    {
        var host = hostString;
        var result;
        string transport = "tcp";

        if (strpos(host, "unix://") === 0) {
            // unix domain
            let result = hostString;
        } else {
            var pos = strpos(host, "://");
            if (pos !== false) {
                let transport = substr(host, 0, pos);
                let host      = substr(host, pos + 3);
            } else {
                let transport = self::DEFAULT_TRANSPORT;
            }
            var tmpMatch;
            if (preg_match("/(.+[^:]):([0-9]+\/*)$/", host, tmpMatch)) {
                let host = tmpMatch[1];
                let port = (int)tmpMatch[2];
            }
            if (strpos(host, "::") !== false) {
                /* ipv6 address should be surrounded brackets */ 
                let host = sprintf("[%s]", trim(host, "[]"));
            }
            let result = sprintf("%s://%s:%d", transport, host, port);
        }
        return result;
    }


    public function post(string tag, array data)
    {
        var sendString = json_encode(data);
        var now = time();
        var packed = json_encode([tag, now, sendString]);
        var buffer = packed;
        int length = strlen(buffer);
        var e;

        try {
            // 接続
            this->connect();

            // 送信
            int written = 0;
            var nwrite = 0;
            int retry = 0;
            var errors;
            while (written < length) {
                let nwrite = this->write(buffer);
                if (is_null(nwrite)) {
                    throw new Exception("connection aborted");
                } elseif (nwrite === 0) {
                    // 書き込めなかった時
                    // リトライは認めない
                    if (!this->options["retry_socket"]) {
                        throw new Exception("simple fluent could not send");
                    }
                    // リトライ回数オーバー
                    if (retry > this->options["max_write_retry"]) {
                        throw new Exception("simple fluent failed send retry: retry count exceeds limit.");
                    }

                    let errors = error_get_last();
                    //  正常時
                    if (!is_null(errors)) {
                        if (array_key_exists("message", errors)) {
                            if (strpos(errors["message"], "errno=32 ") !== false) {
                                // broken pipe
                                this->close();
                                this->connect();
                            } elseif (strpos(errors["message"], "errno=11 ") !== false) {
                            } else {
                                throw new Exception("unknown error : " . errors["message"], errors["type"]);
                            }
                        } else {
                            throw new Exception("unknown error (no message)", errors["type"]);
                        }
                    }
                    // 通信間隔制御
                    if (this->options["backoff_mode"] == self::BACKOFF_TYPE_EXPONENTIAL) {
                        usleep(pow(3, retry) * 1000);
                    } else {
                        usleep(this->options["usleep_wait"]);
                    }
                    // リトライ回数
                    let retry += 1;
                    continue;
                }
                let written += nwrite;
                let buffer = substr(packed, written);
            }
        } catch \Exception, e {
            this->close();
            throw new Exception(e->getMessage(), e->getCode(), e);
        }
    }

    protected function connect()
    {
        if (is_resource(this->socket)) {
            return;
        }

        var connect_options;
        let connect_options = Logger::PHP_STREAM_CLIENT_CONNECT;
        if (this->options["persistent"] === true) {
            let connect_options = connect_options | Logger::PHP_STREAM_CLIENT_PERSISTENT;
        }

        var eno;
        var estr;
        var socket = stream_socket_client(this->transport, eno, estr, this->options["connection_timeout"], connect_options);
        if (!socket) {
            var errors;
            let errors = error_get_last();
            throw new Exception(errors["message"], errors["tyep"]);
        }
        stream_set_timeout(socket, this->options["socket_timeout"]);
        let this->socket = socket;
    }

    public function close()
    {
        if (is_resource(this->socket)) {
            fclose(this->socket);
        }
    }

    protected function write(string buffer)
    {
        var rtn = fwrite(this->socket, buffer);
        if (rtn === false) {
            var errors;
            let errors = error_get_last();
            throw new Exception(errors["message"], errors["type"]);
        }
        return rtn;
    }


}

