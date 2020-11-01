# DATABASE

## XX: 数据隔离

需要对MYSQL设置权限。爬虫数据的采集mysql账号为'robot'，需要管理员
自行添加，数据库主要有account, blog，image，video，其中某些表必须只有一定的
权限。

* account数据库user表'robot'账号只能有查询权限。
* blog数据库'robot'账号只能对category_s和blog_s有增删改查权限。
* image数据库'robot'账号只能对category_s和photo_s有增删改查权限。
* video数据库'robot'账号只能对category_s和video_s有增删改查权限。

主要是为了防止客户端把用户数据删除了。

blog数据库中的相关表
```
CREATE TABLE `article_s` (
  `id` int NOT NULL AUTO_INCREMENT,
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `user_id` int NOT NULL,
  `is_public` tinyint NOT NULL COMMENT '0显示，1隐藏',
  `content` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `date` int NOT NULL,
  `category_id` int NOT NULL,
  `url` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `downloaded` smallint DEFAULT '0',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `title_user_id` (`title`,`user_id`) USING BTREE,
  FULLTEXT KEY `title_content` (`title`,`content`) WITH PARSER `ngram`
) ENGINE=InnoDB AUTO_INCREMENT=72457 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

image数据库库中的相关表
```
CREATE TABLE `photo_s` (
  `id` int NOT NULL AUTO_INCREMENT,
  `category_id` int NOT NULL,
  `date` int NOT NULL,
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `user_id` int NOT NULL,
  `file_extension` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `title_user_id` (`title`,`user_id`) USING BTREE,
  FULLTEXT KEY `title` (`title`) WITH PARSER `ngram`
) ENGINE=InnoDB AUTO_INCREMENT=33 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

video数据库中的相关表
```
CREATE TABLE `video_s` (
  `id` int NOT NULL AUTO_INCREMENT,
  `title` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
  `date` int NOT NULL,
  `file_extension` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
  `user_id` int NOT NULL,
  `category_id` int NOT NULL,
  `description` text CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  PRIMARY KEY (`id`) USING BTREE,
  UNIQUE KEY `title_user_id` (`title`,`user_id`) USING BTREE,
  FULLTEXT KEY `title_description` (`title`,`description`) WITH PARSER `ngram`
) ENGINE=InnoDB AUTO_INCREMENT=77 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
```

## XX: 数据监控

纬度 | 原因 | 价值
:: | :: | ::
浏览次数 | 用户喜好增长率 | 浏览的越多，说明被搜索的越多
评论次数 | 用户喜好增长率 | 评论的越多，内容就越好

根据上面的两个纬度，可以得出大众喜好，然后，提取出商业价值，进行商业决策。