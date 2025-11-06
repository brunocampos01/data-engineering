# Commits

O objetivo de escrever bons commits é facilitar a vida dos reviewers.


# Commit Structure

As mensagens de commit podem ser compostas por **três partes** separadas por uma linha em branco:
- Head ou summary (obrigatório)
- Body (opcional)
- Tail (opcional)

Exemplo usando regex
```
# summary
<type>[optional scope]: <description>

# body
[optional body]

# tail
[optional footer]
```


#### Allow :heavy_check_mark:
- Use **verbos no imperativo** para descrever o que um commit faz, não o que ele fez. Por exemplo, use “mudar”, não “mudou” ou “muda”.


#### Forbidden :no_entry_sign:
- Não use frases como "eu corrigi" ou "eu fiz algo"; é preciso ser claro e informativo.

- Não use `.` ou `…`

- Um commit não é para fazer várias coisas, como por exemplo:
`add new feature, code style, fix a bug ...`
:dizzy_face:
As alterações ficam difíceis de serem enxergadas e torna mais trabalhoso revisar o código.



## Head
Lengt: 50/72 rule is a commonly accepted best practice for formatting commit messages in Git. [Reference](#reference2)

- **`feat: `** um novo recurso.
```sh
git commit -m "feat: add script SQL to create schemas and tables"

git commit -m "feat(runImport): Add new parameter and change function runImport()"
```

<br/>

- **`fix: `** uma correção de bug. Semânticamente, deve estar correlacionado a uma **issue**.
```sh
git commit -m "fix: update HDFS url and Junit version"`
```

<br/>

- **`docs: `** mudanças em documentações.
```sh
git commit -m "doc(readme): improved readability, add new topics, change version node"

git commit -m "style(runImport): Add brackets in variables and script description"
```

<br/>

- **`style: `** formatação, dois pontos ausentes, etc. (não envolve mudança de código).
```sh
git commit -m "style: improve legibility and follow PEP8"
```
<br/>

- **`refactor: `** refatoração do código de produção <br/>
```sh
git commit -m "refactor: change DNS to new hosted zones"
```
<br/>

- **`test: `** adição e refatoração de testes (não envolve mudanças no código de produção) <br/>
```sh
git commit -m "test: add RDS test"
```
<br/>

- **`chore: `** atualização de tarefas de compilação, configurações do gerenciador de pacotes, etc. (não envolve mudanças no código de produção).
```sh
git commit -m "chore: remove emails"

git commit -m "chore: change url of repository"
```
<br/>

- **`BREAKING CHANGE: `** ou **`chore!: `** quebra a compatibilidade.
```sh
git commit -m "chore!: drop Node 6 from testing matrix"

git commit -m "BREAKING CHANGE: dropping Node 6 which hits end of life in April"
```



## Body
Explique o problema que o commit está resolvendo.
Foque no motivo pelo qual está fazendo as mudanças


```
# head
fix: correct minor typos in code

# body
see the issue for details on the typos fixed
```

- Exemplo

<img src="images/body.png" align="middle" height=auto width=100%/>



## Tail
- Mencionar IDs do rastreador de falhas.
- Exemplo
```
Resolve: # 123
Ver também: # 456, # 789
```

- Exemplo completo
```
# head
fix: correct minor typos in code

# body
see the issue for details on the typos fixed

# tail
closes issue #12
```

---

# Pull Request :octocat:

_"Git comes with a nice pull-request generation module, but GitHub instead decided to replace it with their own totally inferior version. As a result, I consider GitHub useless for these kinds of things. It's fine for hosting, but the pull requests and the online commit editing, are just pure garbage."_
**Torvalds**

#### Exemplo

<img src="images/pr.png" align="middle" height=auto width=100%/>

#### Exemplo

If applied, this PR will
- [x] Update version ...
- [x] resolve problem ...
- [x] create test to ...
- [ ] remove ...


#### Lentgh
Pull Request (PR) muito grande, vários commits, vão fazer desenvolvedores perderem muito tempo revisando código, exemplo:

<img src="images/big-pr.png" align="middle" height=auto width=100%/>

- Isso desânima :weary: um code review. :persevere:
- Limite de no máximo a 15 commits
- Se for necessário abra vários PR pequenos que façam sentido semânticamente, como por exemplo:
    - para uma nova feat
    - para code style
    - para um fix

---

## References
- [1] https://semver.org/#summary
- [2] https://www.midori-global.com/blog/2018/04/02/git-50-72-rule
- reference3: https://www.conventionalcommits.org/en/v1.0.0-beta.4/
- [3] guia de estilo Udacity: https://docs.google.com/document/d/1lwthXzmGcTVa-eTnCJEHVDRoCT9ffwvflJeesD82mt4/edit#heading=h.gjdgxs
- [4] https://blog.thoughtram.io/git/2014/11/18/the-anatomy-of-a-git-commit.html

## Author
- Bruno Aurélio Rôzza de Moura Campos (brunocampos01@gmail.com)

## Copyright
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Bruno A. R. M. Campos</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.
