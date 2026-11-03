package cliente

import (
	"bufio"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// LerTexto pede uma linha de texto no terminal, repetindo se vier vazio
func LerTexto(entrada *bufio.Reader, pergunta string) string {
	for {
		fmt.Print(pergunta)
		texto, _ := entrada.ReadString('\n')
		texto = strings.TrimSpace(texto)
		if texto != "" {
			return texto
		}
		fmt.Println("esse campo não pode ficar vazio, tente de novo")
	}
}

// LerData pede uma data DD/MM/AAAA, repetindo até ser válida.
// Devolve o texto digitado (a conversão pro formato interno fica em horario.go)
func LerData(entrada *bufio.Reader, pergunta string) string {
	for {
		texto := LerTexto(entrada, pergunta)
		if _, err := ConverterData(texto); err == nil {
			return texto
		}
		fmt.Println("data inválida, use o formato DD/MM/AAAA")
	}
}

// LerHora pede um horário HH:MM, repetindo até ser válido
func LerHora(entrada *bufio.Reader, pergunta string) string {
	for {
		texto := LerTexto(entrada, pergunta)
		if _, err := ConverterHora(texto); err == nil {
			return texto
		}
		fmt.Println("horário inválido, use o formato HH:MM")
	}
}

// LerPrecoCentavos pede um preço em reais, repetindo até ser válido, e devolve em centavos
func LerPrecoCentavos(entrada *bufio.Reader, pergunta string) int {
	for {
		texto := LerTexto(entrada, pergunta)
		valor, err := strconv.ParseFloat(strings.Replace(texto, ",", ".", 1), 64)
		if err == nil && valor >= 0 {
			return int(math.Round(valor * 100))
		}
		fmt.Println("preço inválido, use algo como 30.50")
	}
}

// LerInteiroPositivo pede um número inteiro maior que zero, repetindo até ser válido
func LerInteiroPositivo(entrada *bufio.Reader, pergunta string) int {
	for {
		texto := LerTexto(entrada, pergunta)
		numero, err := strconv.Atoi(texto)
		if err == nil && numero > 0 {
			return numero
		}
		fmt.Println("digite um número inteiro maior que zero")
	}
}

// LerSimNao pede confirmação s/n, repetindo até o usuário digitar uma das duas opções
func LerSimNao(entrada *bufio.Reader, pergunta string) bool {
	for {
		texto := strings.ToLower(LerTexto(entrada, pergunta))
		if texto == "s" {
			return true
		}
		if texto == "n" {
			return false
		}
		fmt.Println("responda com s ou n")
	}
}

