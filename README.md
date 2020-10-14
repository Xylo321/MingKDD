# reborn-kdd

从reborn项目中分离出的子项目，用于编写爬虫。这些爬虫，需要调用者去使用，具体的使用逻辑由调用者
决定，我只是提供这些爬虫代码，我尽量的将这些代码写的简单通用，调用者通过这些代码拿到数据之后，他们怎么
操作，是他们的事情。

我想，这些爬虫集的唯一宗旨是采集对人类有用的数据，一定是，必须是带有正面能量的东西，尽量不要走歪门邪道的
数据。

然后，采集数据的坑也很多，就如今的反爬技术，只要目标服务器向做，基本就能搞的非常难采集。

目前的反爬措施也就2种比较难一点：
1. 验证码干扰
2. 证据提交

验证码干扰的这种需要借助机器学习来处理。证据提交需要构造证据发送。这两种
都比较复杂，其中机器学习验证码识别，需要一定的数学功底也需要一定的耐心；证据
提交算是比较难的，如果有对方的前端代码，首先比较难的是先要逆向别人的JS，看懂代码
，然后再组装证据发送，另一种是借助模拟组件，如浏览器驱动（无头浏览器），但是
目前目标也能通过识别无头浏览器的指纹来识别，这样就能识别出是爬虫行为，就不给数据。

这两种都是比较难做的。

搞定其中一种，都需要大量的时间，也许是3年以上。

基于上面的这些原因，我就觉得，爬虫有必要被分离出来成为一个独立的项目。

这也是本人为什么会将爬虫集合单独作为一个项目的原因。