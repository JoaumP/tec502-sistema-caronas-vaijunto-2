package cliente

import "fmt"

// ImprimirTitulo mostra um título destacado no terminal
func ImprimirTitulo(titulo string) {
	linha := "══════════════════════════════════════"
	fmt.Println("\n" + linha)
	fmt.Println("  " + titulo)
	fmt.Println(linha)
}

// ImprimirMenu imprime o título, numera as opções a partir de 1 e já deixa o prompt ">"
func ImprimirMenu(titulo string, opcoes []string) {
	ImprimirTitulo(titulo)
	for i, o := range opcoes {
		fmt.Printf("  %d. %s\n", i+1, o)
	}
	fmt.Print("\n> ")
}

// ImprimirSeparador desenha uma linha simples pra separar itens de uma listagem
func ImprimirSeparador() {
	fmt.Println("────────────────────────────────────────")
}
