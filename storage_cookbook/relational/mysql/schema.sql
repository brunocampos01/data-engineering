# Not use  *.sql file out of code because it's necessary use parse to execute statement
# But, if contains many statement is better use file *.sql

CREATE TABLE IF NOT EXISTS `modules`
(
    `cod_module`     int         NOT NULL AUTO_INCREMENT,
    `execution_date` date        NOT NULL,
    `module_name`    varchar(50) NOT NULL,
    `module_status`  boolean     NOT NULL,
    `start_time`     datetime    NOT NULL,
    `end_time`       datetime    NOT NULL,
    `execution_time` time        NOT NULL,
    CONSTRAINT
        `PK_MODULE` PRIMARY KEY (`cod_module`)
) ENGINE = MyISAM
  DEFAULT CHARSET = UTF8;
