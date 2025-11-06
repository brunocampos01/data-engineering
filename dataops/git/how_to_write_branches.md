# Branches

O objetivo de convencionar o nome de branches é para facilitar o entendimento do conteúdo de uma branch e até controlar um ciclo de desenvolvimento.

Ex:
```
name/feature/short-description
```

### Group tokens

Use **tokens de agrupamento** no início dos nomes das branches.

```
wip       Works in progress; stuff I know won't be finished soon
feat      Feature I'm adding or expanding
fix       Bug fix or experiment
junk      Throwaway branch created to experiment
test      Create tests
hotfix    Always branch off Stable
```

### Short well-defined tokens

É possível dizer rapidamente quais branches atingiram cada estágio.

```
$ git branch --list "test/*"
test/foo
test/frabnotz


$ git branch --list "*/foo"
new/foo
test/foo
ver/foo
```

### Use slashes to separate parts
Use barras para separar partes dos nomes das suas ramificações.

```
$ git checkout new<TAB>
Menu:  new/frabnotz   new/foo   new/bar


$ git checkout foo<TAB>
Menu:  new/foo   test/foo   ver/foo
```

Facilita a busca.


### Use underline to separate nomes compostos

Se o seu nome de ramificação é mais do que uma palavra, você deve usar sublinhado '_' ao invés de hífen '-' porque '-' aqui separa os dois branches diferentes.


### Do not use bare numbers
Não use números isolados como descrição principais.
```
$ git checkout CR15032<TAB>
Menu:   fix/CR15032    test/CR15032
```

Dentro da expansão de tabulação de um nome de referência, o git pode decidir que um número faz parte de um nome sha-1 em vez de um nome de uma branch.

---

## References
- [1] https://www.stefanfiott.com/notes/git-notes
- [2] https://stackoverflow.com/questions/273695/what-are-some-examples-of-commonly-used-practices-for-naming-git-branches
- [3]


## Author
- Bruno Aurélio Rôzza de Moura Campos (brunocampos01@gmail.com)

## Copyright
<a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/"><img alt="Creative Commons License" style="border-width:0" src="https://i.creativecommons.org/l/by-sa/4.0/88x31.png" /></a><br />This work by <span xmlns:cc="http://creativecommons.org/ns#" property="cc:attributionName">Bruno A. R. M. Campos</span> is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by-sa/4.0/">Creative Commons Attribution-ShareAlike 4.0 International License</a>.





