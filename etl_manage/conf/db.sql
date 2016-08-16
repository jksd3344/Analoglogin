-- -
-- auth: suguoxin
-- mail: suguoxin@playcrab.com
-- create_time: 2016-04-15 12:30:00
-- used: etl_manage 相关mysql语句
-- -

-- 创建数据库
CREATE DATABASE `etl_manage` DEFAULT CHARACTER SET utf8;

-- 源数据配置表
CREATE TABLE `structure` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `type` varchar(10) DEFAULT NULL COMMENT '数据源类型 1:source 2:dw 3:dm 4:report',
  `flag` enum('log','snap') DEFAULT NULL COMMENT '标注该条数据的类型，如：日志、快照 等',
  `db_type` varchar(10) DEFAULT NULL COMMENT '1:mysql 2:mongo 3:hive',
  `game` varchar(50) DEFAULT NULL COMMENT '游戏简称',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台简称',
  `db_name` varchar(50) DEFAULT NULL COMMENT '数据库',
  `table_name` varchar(50) DEFAULT '' COMMENT '表名',
  `column_name` text COMMENT '字段,逗号隔开',
  `partition_name` varchar(20) DEFAULT NULL COMMENT '分区名，逗号隔开',
  `partition_rule` varchar(20) DEFAULT NULL COMMENT '分区规则',
  `index_name` varchar(20) DEFAULT NULL COMMENT '索引列名',
  `create_table_sql` text COMMENT '创建表语句',
  `user_id` varchar(50) DEFAULT NULL COMMENT '操作者',
  `create_date` date DEFAULT NULL COMMENT '操作时间',
  `is_delete` int(11) DEFAULT '0' COMMENT '是否删除1:删除 0:使用',
  `demo` varchar(250) DEFAULT NULL COMMENT '注释',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 COMMENT='源数据格式的结构记录';

-- etl、download、file2dw、file2mysql、mergefile2dw 任务表
CREATE TABLE `etl_data` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `db_type` enum('mysql','hive','mongo') DEFAULT NULL COMMENT '目标类型',
  `type` enum('download','load') DEFAULT NULL COMMENT 'download:下载源数据文件，load:加载文件',
  `log_name` varchar(50) DEFAULT NULL COMMENT '日志名称',
  `log_dir` varchar(50) DEFAULT NULL COMMENT '日志存放位置',
  `md5_name` varchar(50) DEFAULT NULL COMMENT 'MD5名称',
  `md5_dir` varchar(50) DEFAULT NULL COMMENT 'MD5存放位置',
  `download_url` varchar(255) DEFAULT NULL,
  `do_rate` enum('5min','1hour','1day') DEFAULT NULL COMMENT '执行频率',
  `group` int(11) DEFAULT '10000' COMMENT '组别',
  `priority` int(11) DEFAULT '1' COMMENT '优先级',
  `from_id` int(11) DEFAULT NULL COMMENT 'structure_id 源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structure_id 目标表',
  `prefix_sql` text COMMENT '前置sql',
  `load_sql` text COMMENT '加载sql',
  `post_sql` text COMMENT '后置sql',
  `user_id` int(11) DEFAULT NULL,
  `create_date` date DEFAULT NULL,
  `is_delete` int(11) DEFAULT '0' COMMENT '是否删除，0：否 1：是',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- etl、download 任务日志表
CREATE TABLE `etl_data_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `source_ip` varchar(25) DEFAULT NULL COMMENT '数据的来源，如日志数据的服务器ip、快照数据的区服代号',
  `log_name` varchar(50) DEFAULT NULL COMMENT '日志名称',
  `log_dir` varchar(50) DEFAULT NULL COMMENT '日志存放位置',
  `md5_name` varchar(50) DEFAULT NULL COMMENT 'MD5名称',
  `md5_dir` varchar(50) DEFAULT NULL COMMENT 'MD5存放位置',
  `download_url` varchar(255) DEFAULT NULL,
  `col_num` int(11) DEFAULT NULL COMMENT '列数，用于校验数据完整性',
  `row_num` int(11) DEFAULT NULL COMMENT '文件总条数，用于校验完整性',
  `do_rate` enum('5min','1hour','1day') DEFAULT NULL COMMENT '执行频率',
  `group` int(11) DEFAULT NULL COMMENT '组别',
  `priority` int(11) DEFAULT NULL COMMENT '优先级',
  `from_id` int(11) DEFAULT NULL COMMENT 'structrue源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structrue目标表',
  `log_date` varchar(14) DEFAULT NULL COMMENT '日志的时间',
  `log_time` varchar(25) DEFAULT NULL COMMENT '日志的时间',
  `task_date` varchar(14) DEFAULT NULL COMMENT '任务执行日期',
  `etl_status` int(11) DEFAULT '0' COMMENT '0:初始值 1:开始执行 2:校验完成 3:执行压缩 4:压缩完成 5:执行MD5 6:MD5完成,执行完毕 -1:执行失败 -2:彻底失败，不再自动重试 -3:特殊处理，关闭告警',
  `download_status` int(11) DEFAULT '0' COMMENT '0:初始值 1:开始执行 2:下载完成 3:md5检查完成,执行完毕 -1:执行失败 -2:彻底失败，不再自动重试 -3:特殊处理，关闭告警',
  `etl_exec_num` int(2) DEFAULT '0' COMMENT 'etl执行次数',
  `download_exec_num` int(2) DEFAULT '0' COMMENT 'download执行次数',
  `in_etl_queue` int(2) DEFAULT '0' COMMENT '标记是否在etl队列中 0:否 1:是',
  `in_download_queue` int(2) DEFAULT '0' COMMENT '标记是否在download队列中 0:否 1:是',
  `etl_retry_num` int(2) DEFAULT '0' COMMENT 'etl任务重复执行次数',
  `download_retry_num` int(2) DEFAULT '0' COMMENT 'download重复执行次数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- file2dw、mergefile2dw 任务日志表
CREATE TABLE `file2dw_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `source_ip` varchar(25) DEFAULT NULL COMMENT '数据的来源，如日志数据的服务器ip、快照数据的区服代号',
  `log_name` varchar(50) DEFAULT NULL COMMENT '日志名称',
  `log_dir` varchar(50) DEFAULT NULL COMMENT '日志存放位置',
  `md5_name` varchar(50) DEFAULT NULL COMMENT 'MD5名称',
  `md5_dir` varchar(50) DEFAULT NULL COMMENT 'MD5存放位置',
  `download_url` varchar(255) DEFAULT NULL,
  `col_num` int(11) DEFAULT NULL COMMENT '列数，用于校验数据完整性',
  `row_num` int(11) DEFAULT NULL COMMENT '文件总条数，用于校验完整性',
  `do_rate` enum('5min','1hour','1day') DEFAULT '1hour' COMMENT '执行频率',
  `group` int(11) DEFAULT NULL COMMENT '组别',
  `priority` int(11) DEFAULT NULL COMMENT '优先级',
  `from_id` int(11) DEFAULT NULL COMMENT 'structrue源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structrue目标表',
  `prefix_sql` text COMMENT '前置sql',
  `load_sql` text COMMENT 'file2dw 执行sql',
  `post_sql` text COMMENT '后置sql',
  `log_date` varchar(14) DEFAULT NULL COMMENT '日志的时间',
  `log_time` varchar(25) DEFAULT NULL COMMENT '日志的时间',
  `task_date` varchar(14) DEFAULT NULL COMMENT '任务执行日期',
  `load_status` int(11) DEFAULT '0' COMMENT '0:初始值 1:开始执行 2:合并完成 3:压缩完成 4:load完成 5:建立索引完成,执行完毕 -1:执行失败 -3:特殊处理，关闭告警',
  `exec_num` int(2) DEFAULT '0' COMMENT '执行次数',
  `in_queue` int(2) DEFAULT '0' COMMENT '标记是否在队列中 0:否 1:是',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- file2mysql 任务日志表
CREATE TABLE `file2mysql_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `source_ip` varchar(25) DEFAULT NULL COMMENT '数据的来源，如日志数据的服务器ip、快照数据的区服代号',
  `log_name` varchar(50) DEFAULT NULL COMMENT '日志名称',
  `log_dir` varchar(50) DEFAULT NULL COMMENT '日志存放位置',
  `md5_name` varchar(50) DEFAULT NULL COMMENT 'MD5名称',
  `md5_dir` varchar(50) DEFAULT NULL COMMENT 'MD5存放位置',
  `download_url` varchar(255) DEFAULT NULL,
  `col_num` int(11) DEFAULT NULL COMMENT '列数，用于校验数据完整性',
  `row_num` int(11) DEFAULT NULL COMMENT '文件总条数，用于校验完整性',
  `do_rate` enum('5min','1hour','1day') DEFAULT '5min' COMMENT '执行频率',
  `group` int(11) DEFAULT NULL COMMENT '组别',
  `priority` int(11) DEFAULT NULL COMMENT '优先级',
  `from_id` int(11) DEFAULT NULL COMMENT 'structrue源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structrue目标表',
  `prefix_sql` text COMMENT '前置sql',
  `load_sql` text COMMENT 'file2mysql 执行sql',
  `post_sql` text COMMENT '后置sql',
  `log_date` varchar(14) DEFAULT NULL COMMENT '日志的时间',
  `log_time` varchar(25) DEFAULT NULL COMMENT '日志的时间',
  `task_date` varchar(14) DEFAULT NULL COMMENT '任务执行日期',
  `load_status` int(11) DEFAULT '0' COMMENT '0:初始化值 1:正在执行 2:执行完成',
  `exec_num` int(2) DEFAULT '0' COMMENT '执行次数',
  `in_queue` int(2) DEFAULT '0' COMMENT '标记是否在队列中 0:否 1:是',
  `retry_num` int(2) DEFAULT '0' COMMENT '任务重试次数',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- dw2dm 任务表
CREATE TABLE `dw2dm` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `log_name` varchar(50) DEFAULT NULL COMMENT '数据表名',
  `do_rate` enum('1day','7day') DEFAULT NULL COMMENT '执行频次',
  `grouped` int(11) DEFAULT '1' COMMENT '组别',
  `priority` int(11) DEFAULT '1' COMMENT '基于组别的优先级',
  `from_id` varchar(20) DEFAULT NULL COMMENT 'structure_id 源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structure_id 目标表',
  `prefix_sql` text COMMENT '前置sql',
  `exec_sql` text COMMENT '执行sql',
  `post_sql` text COMMENT '后置sql',
  `is_delete` tinyint(1) DEFAULT '0' COMMENT '是否删除',
  `user_id` varchar(20) DEFAULT NULL COMMENT '创建人',
  `create_date` date DEFAULT NULL COMMENT '创建日期',
  `comment` varchar(50) DEFAULT NULL COMMENT '该条任务的注释',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- dw2dm 任务日志表
CREATE TABLE `dw2dm_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `log_name` varchar(50) DEFAULT NULL COMMENT '数据表名',
  `do_rate` enum('1day','7day') DEFAULT NULL COMMENT '执行频次',
  `grouped` int(11) DEFAULT '1' COMMENT '组别',
  `priority` int(11) DEFAULT '1' COMMENT '基于组别的优先级',
  `from_id` varchar(20) DEFAULT NULL COMMENT 'structure_id 源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structure_id 目标表',
  `prefix_sql` text COMMENT '前置sql',
  `exec_sql` text COMMENT '执行sql',
  `post_sql` text COMMENT '后置sql',
  `log_date` varchar(14) DEFAULT NULL COMMENT '日志日期',
  `task_date` varchar(14) DEFAULT NULL COMMENT '任务执行日期',
  `status` int(2) DEFAULT '0' COMMENT '0:初始化值 1:正在执行 2:执行完成',
  `exec_num` int(2) DEFAULT '0' COMMENT '执行次数',
  `in_queue` int(2) DEFAULT '0' COMMENT '模拟是否在队列中，0:否，1:是',
  `start_time` varchar(25) DEFAULT NULL COMMENT '开始时间',
  `end_time` varchar(25) DEFAULT NULL COMMENT '结束时间',
  `comment` varchar(50) DEFAULT NULL COMMENT '该条任务的注释',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- dm2report 任务表
CREATE TABLE `dm2report` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `task_name` varchar(50) DEFAULT NULL COMMENT '任务英文名',
  `date_cycle` int(11) DEFAULT NULL COMMENT '数据的时间周期',
  `do_rate` enum('1day','7day','30day') DEFAULT NULL COMMENT '执行频率',
  `group` int(11) DEFAULT '1' COMMENT '组别',
  `priority` int(11) DEFAULT '10000' COMMENT '优先级',
  `prefix_sql` text COMMENT '前置SQL',
  `exec_sql` text COMMENT '要执行的sql',
  `post_sql` text COMMENT '后置SQL',
  `from_id` varchar(50) DEFAULT NULL COMMENT 'structure_id 源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structure_id 目标表',
  `is_delete` int(11) DEFAULT '0' COMMENT '是否删除，0:否 1:是',
  `user_id` varchar(20) DEFAULT NULL COMMENT '创建者',
  `create_date` date DEFAULT NULL COMMENT '创建时间',
  `comment` varchar(50) DEFAULT NULL COMMENT '该条任务的注释',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;

-- dm2report 任务日志表
CREATE TABLE `dm2report_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `game` varchar(50) DEFAULT NULL COMMENT '游戏',
  `platform` varchar(50) DEFAULT NULL COMMENT '平台',
  `task_name` varchar(50) DEFAULT NULL COMMENT '任务英文名',
  `date_cycle` int(11) DEFAULT NULL COMMENT '数据的时间周期',
  `do_rate` enum('1day','7day','30day') DEFAULT NULL COMMENT '执行频率',
  `group` int(11) DEFAULT '1' COMMENT '组别',
  `priority` int(11) DEFAULT '10000' COMMENT '优先级',
  `prefix_sql` text COMMENT '前置SQL',
  `exec_sql` text COMMENT '要执行的sql',
  `post_sql` text COMMENT '后置SQL',
  `tmp_file_name` varchar(100) DEFAULT NULL,
  `from_id` varchar(50) DEFAULT NULL COMMENT 'structure_id 源表',
  `target_id` int(11) DEFAULT NULL COMMENT 'structure_id 目标表',
  `log_date` varchar(14) DEFAULT NULL COMMENT '日志的日期',
  `task_date` varchar(14) DEFAULT NULL COMMENT '任务执行日期',
  `status` int(11) DEFAULT '0' COMMENT '0:初始值 1:开始执行 2:删除陈旧数据完成 3:执行hql dump数据完成 4:录入mysql完成,执行完毕 -1:执行失败 -3:特殊处理，关闭告警',
  `exec_num` int(11) DEFAULT '0',
  `in_queue` int(11) DEFAULT '0',
  `start_time` varchar(20) DEFAULT NULL COMMENT '任务开始时间',
  `end_time` varchar(20) DEFAULT NULL COMMENT '任务完成时间',
  `comment` varchar(50) DEFAULT NULL COMMENT '该条任务的注释',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;