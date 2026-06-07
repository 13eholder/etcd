# Git 协作
只允许在自己的分支上进行更新，不得直接 push master。应该使用 pull request 进行合并，并至少需要一人审阅。发起 pull request 时，需保证无警告无报错

git commit message 格式（仅第一行必须，方括号内为给定的一个词语或者几个词语。几个词语则用逗号隔开，逗号后有空格）, XXX 为提交信息总结, YYY/ZZZ 是markdown格式的详细信息
```
[docs/chores/feat/fix] XXXXXXXX

YYYYYY
ZZZZZZ
......
```

常见的提交范式:
- feat(新功能)(feature)
    - feat: 增加用户注册功能
- fix(修复bug)
    - fix: 修复页面登录崩溃的问题
- docs(文档变更)
    - docs: 更新README文件
- style(代码风格变更)
    - style: 删除多余空行
- refactor(代码重构)
    - refactor: 重构用户验证逻辑
- perf(性能优化)
    - perf: 加载图片加载速度
- test(添加或修改测试)
    - test: 增加用户模块测试
- chore: (杂项:构建过程或辅助工具的变动)
    - chore:更新依赖库
- build: 构建系统或外部依赖项的变更
    - build: 升级webpack到版本5
- ci: 持续集成配置的变更
    - ci: 例如修改Github Actions配置文件
- revert: 回滚之前的提交
    - revert: 回滚到feat: 增加用户注册功能

当你在自己的分支（例如：cyx），同时其他程序员更新了 master，你想要将 master 更改合并到自己的 cyx 分支时，可以使用：
```
git checkout master # 从 cyx 分支改到 master 分支
git pull origin master # 快速更新 master 分支
git checkout cyx # 回到自己分支
git rebase master # 合入 master 的更改
git push origin cyx # push 到远端 cyx 分支
# 如果 push 失败：
git push --force-with-lease origin cyx
```