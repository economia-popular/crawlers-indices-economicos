package sociais

import (
	"crawlers/pkg/logger"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
)

func RunnerCadastroUnicoQuilombolasPobreza() {

	runnerName := "Quilombolas Pobreza"
	domain := "aplicacoes.cidadania.gov.br"
	url := "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q%5B%5D=oNOclsLerpibuKep3bV%2Bfmdl05Kv2rmg2a19ZW51ZXKmaX6JaV2Jk2CadmCNrMmTdKemr%2BKlrLGYkbaYY8luZ422s5Wou5p135q5wZxokseU1rCYmLbAqanEe5vm%2FPq6oI7KgYTfpp%2FM6K%2BjqKmoWt6mbcGgoczC9hEA1sybsZlcuKSc657Hr1eWxdSW3Kanvu5toqtoeJvdmsDCqZx3JM3YppbM971v"
	unidadeMedida := "Familias Quilombolas em Situação de Pobreza"
	fonte := "https://aplicacoes.cidadania.gov.br"
	file_path := "./data/sociais/quilombolas_pobreza_cadastro_unico.json"
	fileNameOutputCSV := "./data/sociais/quilombolas_pobreza_cadastro_unico.csv"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
	)

	indice := &Pobreza{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = fonte
	indice.UnidadeMedida = unidadeMedida

	c.OnHTML("table", func(e *colly.HTMLElement) {

		l.Info().
			Str("Runner", runnerName).
			Str("Domain", domain).
			Str("URL", url).
			Msg("Recuperando o HTML da Página")

		l.Info().
			Str("Runner", runnerName).
			Str("Domain", domain).
			Str("URL", url).
			Msg("Encontrando o elemento <table> para efetuar o parsing")

		e.ForEach("tr", func(i int, tr *colly.HTMLElement) {

			referencia_td := strings.Replace(tr.ChildText("td:nth-child(1)"), ".", "", -1)
			valor_td := strings.Replace(tr.ChildText("td:nth-child(2)"), ".", "", -1)
			valor, _ := strconv.ParseInt(strings.TrimSpace(valor_td), 10, 64)

			item := DataPobreza{
				Referencia: referencia_td,
				Total:      valor,
			}

			if valor > 0 {
				l.Info().
					Str("Runner", runnerName).
					Str("Domain", domain).
					Str("Referencia", referencia_td).
					Int64("Numero de Pessoas em Pobreza/Extrema Pobreza", valor).
					Msg("Adicionando item ao dataset")

				indice.Data = append(indice.Data, item)
			}

		})

		l.Info().
			Str("Runner", runnerName).
			Msg("Convertendo a Struct do Schema em formato JSON")

		b, err := json.Marshal(indice)
		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Msg("Erro ao converter a struct em JSON")
		}

		l.Info().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Msg("Criando arquivo de persistência para os dados convertidos")

		f, err := os.Create(file_path)
		defer f.Close()

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("FilePath", file_path).
				Str("Error", err.Error()).
				Msg("Erro ao criar o diretório para persistência dos dados")
		}

		l.Info().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Msg("Iniciando a escrita dos dados no arquivo de persistência")

		_, err = f.WriteString(string(b))

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("FilePath", file_path).
				Str("Error", err.Error()).
				Msg("Erro para escrever os dados no arquivo")
		}

		// Convertendo em CSV
		csvFile, err := os.OpenFile(fileNameOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("FilePath", fileNameOutputCSV).
				Str("Error", err.Error()).
				Msg("Erro ao criar o dataset em CSV")
		}
		defer csvFile.Close()

		csvOutput, err := gocsv.MarshalString(&indice.Data)
		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("FilePath", fileNameOutputCSV).
				Str("Error", err.Error()).
				Msg("Erro ao escrever o dataset em CSV")
		}

		_, err = csvFile.WriteString(string(csvOutput))
		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("FilePath", fileNameOutputCSV).
				Str("Error", err.Error()).
				Msg("Erro para escrever os dados no arquivo")
		}

		l.Info().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Msg("Dataset em CSV Criado")

		l.Info().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Msg("Finalizado")

		l.Info().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Msg("Finalizado")

	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)

}
