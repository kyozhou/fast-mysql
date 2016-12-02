<?php
namespace lib;

class DB {

    private $host;
    private $user;
    private $password;
    private $port;
    private $charset;
    private $databaseName;
    private $pdoObj;
    private static $dbInstances;

    static function get($config) {
        $dbKey = md5(serialize($config));
        if (!isset(self::$dbInstances[$dbKey])) {
            self::$dbInstances[$dbKey] = new DB($config);
        }
        return self::$dbInstances[$dbKey];
    }

    function __clone() {
        trigger_error('Clone is not allow', E_USER_ERROR);
    }

    function __construct($config) {
        $this->host = $config['host'];
        $this->port = $config['port'];
        $this->user = $config['user'];
        $this->password = $config['password'];
        $this->databaseName = $config['name'];

        if (isset($config['charset']) && $config['charset']) {
            $this->charset = $config['charset'];
        } else {
            $this->charset = 'utf8';
        }

        $this->connect();
    }

    function __destruct() {
        $this->close();
    }

    function connect() {
        $this->close();
        try {
            $this->pdoObj = new \PDO('mysql:host='. $this->host .';port='. $this->port .';dbname=' . $this->databaseName,
            $this->user,
            $this->password,
            array(
                \PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES '. $this->charset,
                \PDO::ATTR_EMULATE_PREPARES => false,
                \PDO::ATTR_STRINGIFY_FETCHES => false
            ));
        } catch (PDOException $e) {
            echo 'Connection failed: ' . $e->getMessage();
        }
    }

    function close() {
        $this->pdoObj = null;
    }

    function beginTransaction() {
        $this->pdoObj->beginTransaction();
    }

    function commit() {
        $this->pdoObj->commit();
    }

    function rollback() {
        $this->pdoObj->rollBack();
    }

    function execute($sql, $args = []) {
        $statement = $this->pdoObj->prepare($sql);
        if($statement) {
            $this->bindParams($statement, $args);
            $statement->execute($args);
            return $statement->rowCount();
        }else {
            return false;
        }
    }

    function insert($sql, $args = [], $key = null) {
        $statement = $this->pdoObj->prepare($sql);
        if($statement) {
            $this->bindParams($statement, $args);
            $statement->execute($args);
            $lastInsertId = $key == null ? $this->pdoObj->lastInsertId() : $this->pdoObj->lastInsertId($key);
            return (int)$lastInsertId;
        }else {
            return false;
        }
    }

    function fetchTable($sql, $args = []) {
        $statement = $this->pdoObj->prepare($sql);
        if($statement) {
            $this->bindParams($statement, $args);
            $statement->execute($args);
            $result = $statement->fetchAll(\PDO::FETCH_ASSOC);
            return $result;
        }else {
            return false;
        }
    }

    function fetchRow($sql, $args = []) {
        $statement = $this->pdoObj->prepare($sql);
        if($statement) {
            $this->bindParams($statement, $args);
            $statement->execute($args);
            $result = $statement->fetch(\PDO::FETCH_ASSOC);
            return $result;
        }else {
            return false;
        }
    }

    function fetchColumn($sql, $args = []) {
        $statement = $this->pdoObj->prepare($sql);
        if($statement) {
            $this->bindParams($statement, $args);
            $statement->execute($args);
            $column = [];
            $result = $statement->fetchColumn();
            while($result !== false) {
                $column[] = $result;
                $result = $statement->fetchColumn();
            }
            return $column;
        }else {
            return false;
        }
    }

    function fetchCell($sql, $args = []) {
        $statement = $this->pdoObj->prepare($sql);
        if($statement) {
            $this->bindParams($statement, $args);
            $statement->execute($args);
            $result = $statement->fetch(\PDO::FETCH_NUM);
            return empty($result[0]) ? null : $result[0];
        }else {
            return false;
        }
    }

    function bindParams(&$statement, &$args) {
        foreach($args as $index => &$arg) {
            if(is_int($arg)) {
                $statement->bindValue($index+1, (int)$arg, \PDO::PARAM_INT);
            }else {
                $statement->bindValue($index+1, $arg, \PDO::PARAM_STR);
            }
        }
    }
}
