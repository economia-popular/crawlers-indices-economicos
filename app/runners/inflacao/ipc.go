package inflacao

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
)

type DataIPC struct {
	Referencia   string  `json:"referencia" csv:"referencia"`
	Ano          string  `json:"ano" csv:"ano"`
	Mes          string  `json:"mes" csv:"mes"`
	Anomes       string  `json:"ano_mes csv:"ano_mes"`
	Variacao     float64 `json:"variacao" csv:"variacao"`
	AcumuladoAno float64 `json:"acumulado_ano" csv:"acumulado_ano"`
}

type IPC struct {
	Atualizacao   time.Time `json:"data_atualizacao"`
	UnidadeMedida string    `json:"unidade_medida"`
	Fonte         string    `json:"fonte"`
	Data          []DataIPC `json:"data"`
}

func RunnerIPC() {
	runnerName := "IPC-FIPE"
	domain := "www.debit.com.br"
	url := "https://www.debit.com.br/tabelas/tabela-completa.php?indice=ipc_fipe"
	file_path := "./data/inflacao/ipc-fipe.json"
	fileNameOutputCSV := "./data/inflacao/ipc-fipe.csv"

	s3KeyCSV := "inflacao/ipc-fipe.csv"
	s3KeyJSON := "inflacao/ipc-fipe.json"

	acc := []DataIPC{}

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector()

	indice := &IPC{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = url

	// Find and print all links
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
			referencia_td := tr.ChildText("td:nth-child(1)")
			valor_td := strings.Replace(tr.ChildText("td:nth-child(2)"), ",", ".", -1)

			// Ignorando empty values
			if len([]rune(referencia_td)) <= 1 {
				return
			}

			valor, _ := strconv.ParseFloat(strings.TrimSpace(valor_td), 10)

			ano := referencia_td[3:7]
			mes := referencia_td[0:2]
			anomes := fmt.Sprintf("%s%s", ano, mes)

			referencia := fmt.Sprintf("%s-%s", ano, mes)

			item := DataIPC{
				Variacao:   valor,
				Ano:        ano,
				Mes:        mes,
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)
		})
	})

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Efetuando requisição para o Endpoint")

	c.Visit(url)

	l.Info().
		Str("Runner", runnerName).
		Str("Domain", domain).
		Str("URL", url).
		Msg("Construindo o acomulado")

	for i, k := range acc {

		// Ignorando o Acumulado Ano
		if k.Mes == "01" {
			k.AcumuladoAno = k.Variacao
			indice.Data = append(indice.Data, k)
			continue
		} else {
			l := indice.Data[i-1]
			acumulado := l.AcumuladoAno + k.Variacao
			k.AcumuladoAno = math.Round(acumulado*100) / 100
		}

		indice.Data = append(indice.Data, k)
	}

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
		Str("S3Key", s3KeyCSV).
		Msg("Fazendo Upload para o S3")

	err = upload.S3(fileNameOutputCSV, s3KeyCSV)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("S3Key", s3KeyCSV).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	err = upload.S3(file_path, s3KeyJSON)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", file_path).
			Str("S3Key", s3KeyJSON).
			Str("Error", err.Error()).
			Msg("Erro ao fazer upload do arquivo para o S3")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameOutputCSV).
		Msg("Finalizado")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", file_path).
		Msg("Finalizado")
}
