package inflacao

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
)

func RunnerINCCM2008() {
	runnerName := "INCC-M - 1989-2008"
	domain := "www.yahii.com.br"
	url := "http://www.yahii.com.br/inccM1989a2008.html"
	file_path := "./data/inflacao/incc-m-1989-2008.json"
	fileNameOutputCSV := "./data/inflacao/incc-m-1989-2008.csv"

	s3KeyCSV := "inflacao/incc-m-1989-2008.csv"
	s3KeyJSON := "inflacao/incc-m-1989-2008.json"

	acc := []DataINCC{}

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector()

	indice := &INCC{}

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

			// fmt.Println(tr)

			ano_raw := tr.ChildText("td:nth-child(1)")
			ano, _ := strconv.ParseInt(strings.TrimSpace(ano_raw), 10, 64)

			if ano == 0 {
				return
			}

			janeiro_raw := strings.Replace(tr.ChildText("td:nth-child(2)"), ",", ".", -1)
			janeiro_raw = strings.Replace(janeiro_raw, "%", "", -1)
			janeiro, _ := strconv.ParseFloat(strings.TrimSpace(janeiro_raw), 10)

			fevereiro_raw := strings.Replace(tr.ChildText("td:nth-child(3)"), ",", ".", -1)
			fevereiro_raw = strings.Replace(fevereiro_raw, "%", "", -1)
			fevereiro, _ := strconv.ParseFloat(strings.TrimSpace(fevereiro_raw), 10)

			marco_raw := strings.Replace(tr.ChildText("td:nth-child(4)"), ",", ".", -1)
			marco_raw = strings.Replace(marco_raw, "%", "", -1)
			marco, _ := strconv.ParseFloat(strings.TrimSpace(marco_raw), 10)

			abril_raw := strings.Replace(tr.ChildText("td:nth-child(5)"), ",", ".", -1)
			abril_raw = strings.Replace(abril_raw, "%", "", -1)
			abril, _ := strconv.ParseFloat(strings.TrimSpace(abril_raw), 10)

			maio_raw := strings.Replace(tr.ChildText("td:nth-child(6)"), ",", ".", -1)
			maio_raw = strings.Replace(maio_raw, "%", "", -1)
			maio, _ := strconv.ParseFloat(strings.TrimSpace(maio_raw), 10)

			junho_raw := strings.Replace(tr.ChildText("td:nth-child(7)"), ",", ".", -1)
			junho_raw = strings.Replace(junho_raw, "%", "", -1)
			junho, _ := strconv.ParseFloat(strings.TrimSpace(junho_raw), 10)

			julho_raw := strings.Replace(tr.ChildText("td:nth-child(8)"), ",", ".", -1)
			julho_raw = strings.Replace(julho_raw, "%", "", -1)
			julho, _ := strconv.ParseFloat(strings.TrimSpace(julho_raw), 10)

			agosto_raw := strings.Replace(tr.ChildText("td:nth-child(9)"), ",", ".", -1)
			agosto_raw = strings.Replace(agosto_raw, "%", "", -1)
			agosto, _ := strconv.ParseFloat(strings.TrimSpace(agosto_raw), 10)

			setembro_raw := strings.Replace(tr.ChildText("td:nth-child(10)"), ",", ".", -1)
			setembro_raw = strings.Replace(setembro_raw, "%", "", -1)
			setembro, _ := strconv.ParseFloat(strings.TrimSpace(setembro_raw), 10)

			outubro_raw := strings.Replace(tr.ChildText("td:nth-child(11)"), ",", ".", -1)
			outubro_raw = strings.Replace(outubro_raw, "%", "", -1)
			outubro, _ := strconv.ParseFloat(strings.TrimSpace(outubro_raw), 10)

			novembro_raw := strings.Replace(tr.ChildText("td:nth-child(12)"), ",", ".", -1)
			novembro_raw = strings.Replace(novembro_raw, "%", "", -1)
			novembro, _ := strconv.ParseFloat(strings.TrimSpace(novembro_raw), 10)

			dezembro_raw := strings.Replace(tr.ChildText("td:nth-child(13)"), ",", ".", -1)
			dezembro_raw = strings.Replace(dezembro_raw, "%", "", -1)
			dezembro, _ := strconv.ParseFloat(strings.TrimSpace(dezembro_raw), 10)

			// Janeiro
			mes := "01"
			anomes := fmt.Sprintf("%v%s", ano, mes)
			referencia := fmt.Sprintf("%v-%s", ano, mes)

			item := DataINCC{
				Variacao:   janeiro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Fevereiro
			mes = "02"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   fevereiro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Março
			mes = "03"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   marco,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Abril
			mes = "04"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   abril,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Maio
			mes = "05"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   maio,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Junho
			mes = "06"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   junho,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Julho
			mes = "07"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   julho,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Agosto
			mes = "08"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   agosto,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Setembro
			mes = "09"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   setembro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Outubro
			mes = "10"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   outubro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Novembro
			mes = "11"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   novembro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Dezembro
			mes = "12"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   dezembro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
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
		if k.Mes == "01" || i == 0 {
			k.AcumuladoAno = k.Variacao
			indice.Data = append(indice.Data, k)
			continue
		} else {
			l := indice.Data[len(indice.Data)-1]
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

func RunnerINCCM() {
	runnerName := "INCC-M"
	domain := "www.yahii.com.br"
	url := "http://www.yahii.com.br/inccM.html"
	file_path := "./data/inflacao/incc-m-2009-atual.json"
	fileNameOutputCSV := "./data/inflacao/incc-m-2009-atual.csv"

	s3KeyCSV := "inflacao/incc-m-2009-atual.csv"
	s3KeyJSON := "inflacao/incc-m-2009-atual.json"

	acc := []DataINCC{}

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector()

	indice := &INCC{}

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

			// fmt.Println(tr)

			ano_raw := tr.ChildText("td:nth-child(1)")
			ano, _ := strconv.ParseInt(strings.TrimSpace(ano_raw), 10, 64)

			if ano == 0 {
				return
			}

			janeiro_raw := strings.Replace(tr.ChildText("td:nth-child(2)"), ",", ".", -1)
			janeiro_raw = strings.Replace(janeiro_raw, "%", "", -1)
			janeiro, _ := strconv.ParseFloat(strings.TrimSpace(janeiro_raw), 10)

			fevereiro_raw := strings.Replace(tr.ChildText("td:nth-child(3)"), ",", ".", -1)
			fevereiro_raw = strings.Replace(fevereiro_raw, "%", "", -1)
			fevereiro, _ := strconv.ParseFloat(strings.TrimSpace(fevereiro_raw), 10)

			marco_raw := strings.Replace(tr.ChildText("td:nth-child(4)"), ",", ".", -1)
			marco_raw = strings.Replace(marco_raw, "%", "", -1)
			marco, _ := strconv.ParseFloat(strings.TrimSpace(marco_raw), 10)

			abril_raw := strings.Replace(tr.ChildText("td:nth-child(5)"), ",", ".", -1)
			abril_raw = strings.Replace(abril_raw, "%", "", -1)
			abril, _ := strconv.ParseFloat(strings.TrimSpace(abril_raw), 10)

			maio_raw := strings.Replace(tr.ChildText("td:nth-child(6)"), ",", ".", -1)
			maio_raw = strings.Replace(maio_raw, "%", "", -1)
			maio, _ := strconv.ParseFloat(strings.TrimSpace(maio_raw), 10)

			junho_raw := strings.Replace(tr.ChildText("td:nth-child(7)"), ",", ".", -1)
			junho_raw = strings.Replace(junho_raw, "%", "", -1)
			junho, _ := strconv.ParseFloat(strings.TrimSpace(junho_raw), 10)

			julho_raw := strings.Replace(tr.ChildText("td:nth-child(8)"), ",", ".", -1)
			julho_raw = strings.Replace(julho_raw, "%", "", -1)
			julho, _ := strconv.ParseFloat(strings.TrimSpace(julho_raw), 10)

			agosto_raw := strings.Replace(tr.ChildText("td:nth-child(9)"), ",", ".", -1)
			agosto_raw = strings.Replace(agosto_raw, "%", "", -1)
			agosto, _ := strconv.ParseFloat(strings.TrimSpace(agosto_raw), 10)

			setembro_raw := strings.Replace(tr.ChildText("td:nth-child(10)"), ",", ".", -1)
			setembro_raw = strings.Replace(setembro_raw, "%", "", -1)
			setembro, _ := strconv.ParseFloat(strings.TrimSpace(setembro_raw), 10)

			outubro_raw := strings.Replace(tr.ChildText("td:nth-child(11)"), ",", ".", -1)
			outubro_raw = strings.Replace(outubro_raw, "%", "", -1)
			outubro, _ := strconv.ParseFloat(strings.TrimSpace(outubro_raw), 10)

			novembro_raw := strings.Replace(tr.ChildText("td:nth-child(12)"), ",", ".", -1)
			novembro_raw = strings.Replace(novembro_raw, "%", "", -1)
			novembro, _ := strconv.ParseFloat(strings.TrimSpace(novembro_raw), 10)

			dezembro_raw := strings.Replace(tr.ChildText("td:nth-child(13)"), ",", ".", -1)
			dezembro_raw = strings.Replace(dezembro_raw, "%", "", -1)
			dezembro, _ := strconv.ParseFloat(strings.TrimSpace(dezembro_raw), 10)

			// Janeiro
			mes := "01"
			anomes := fmt.Sprintf("%v%s", ano, mes)
			referencia := fmt.Sprintf("%v-%s", ano, mes)

			item := DataINCC{
				Variacao:   janeiro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Fevereiro
			mes = "02"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   fevereiro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Março
			mes = "03"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   marco,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Abril
			mes = "04"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   abril,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Maio
			mes = "05"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   maio,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Junho
			mes = "06"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   junho,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Julho
			mes = "07"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   julho,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Agosto
			mes = "08"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   agosto,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Setembro
			mes = "09"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   setembro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Outubro
			mes = "10"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   outubro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Novembro
			mes = "11"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   novembro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
				Anomes:     anomes,
				Referencia: referencia,
			}

			acc = append(acc, item)

			// Dezembro
			mes = "12"
			anomes = fmt.Sprintf("%v%s", ano, mes)
			referencia = fmt.Sprintf("%v-%s", ano, mes)

			item = DataINCC{
				Variacao:   dezembro,
				Ano:        fmt.Sprintf("%v", ano),
				Mes:        fmt.Sprintf("%v", mes),
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

		if k.Variacao == 0 {
			continue
		}

		// Ignorando o Acumulado Ano
		if k.Mes == "01" || i == 0 {
			k.AcumuladoAno = k.Variacao
			indice.Data = append(indice.Data, k)
			continue
		} else {
			l := indice.Data[len(indice.Data)-1]
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

func RunnerINCCConsolidado() {

	runnerName := "INCC-M - Consolidacao"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	file_path_1989 := "./data/inflacao/incc-m-1989-2008.json"
	file_path_atual := "./data/inflacao/incc-m-2009-atual.json"

	domain := "www.yahii.com.br"

	file_path := "./data/inflacao/incc-m.json"
	fileNameOutputCSV := "./data/inflacao/incc-m.csv"

	s3KeyCSV := "inflacao/incc-m.csv"
	s3KeyJSON := "inflacao/incc-m.json"

	now := time.Now()
	indice := &INCC{}
	indice.Fonte = domain
	indice.Atualizacao = now

	// 1989 - 2008
	INCCM1989 := INCC{}
	file1989, err := ioutil.ReadFile(file_path_1989)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", file_path_1989).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(file1989), &INCCM1989)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", file_path_1989).
			Msg("converter para struct")
	}

	// 2009 - Atual
	INCCMAtual := INCC{}
	fileAtual, err := ioutil.ReadFile(file_path_atual)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", file_path_atual).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileAtual), &INCCMAtual)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", file_path_atual).
			Msg("converter para struct")
	}

	indice.Data = append(indice.Data, INCCM1989.Data...)
	indice.Data = append(indice.Data, INCCMAtual.Data...)

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
