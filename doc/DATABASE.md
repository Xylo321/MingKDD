# DATABASE

需要对MYSQL设置权限。爬虫数据的采集mysql账号为'robot'，需要管理员
自行添加，数据库主要有account, blog，image，video，其中某些表必须只有一定的
权限。

* account数据库user表'robot'账号只能有查询权限。
* blog数据库'robot'账号只能对category_s和blog_s有增删改查权限。
* image数据库'robot'账号只能对category_s和photo_s有增删改查权限。
* video数据库'robot'账号只能对category_s和video_s有增删改查权限。

主要是为了防止客户端把用户数据删除了。