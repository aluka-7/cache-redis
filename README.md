# 缓存Redis单实例

配置中心的配置路径为：/system/base/cache/provider

基于Redis单实例的缓存实现(单机），配置中心或构造方法中参数的配置格式如下：
```
{
    "host" : "缓存服务器主机地址，必须",
    "port" : "缓存服务器端口号",
    "sasl" : "是否开启安全认证，true|false，可选，默认是没有",
    "password" : "开启安全认证后的登录密码，sasl如果指定为true则必须"
 }
```
