package pib

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

type ResponsePerCapta []struct {
	ID         string `json:"id"`
	Variavel   string `json:"variavel"`
	Unidade    string `json:"unidade"`
	Resultados []struct {
		Classificacoes []interface{} `json:"classificacoes"`
		Series         []struct {
			Localidade struct {
				ID    string `json:"id"`
				Nivel struct {
					ID   string `json:"id"`
					Nome string `json:"nome"`
				} `json:"nivel"`
				Nome string `json:"nome"`
			} `json:"localidade"`
			Serie struct {
				Num1996 string `json:"1996"`
				Num1997 string `json:"1997"`
				Num1998 string `json:"1998"`
				Num1999 string `json:"1999"`
				Num2000 string `json:"2000"`
				Num2001 string `json:"2001"`
				Num2002 string `json:"2002"`
				Num2003 string `json:"2003"`
				Num2004 string `json:"2004"`
				Num2005 string `json:"2005"`
				Num2006 string `json:"2006"`
				Num2007 string `json:"2007"`
				Num2008 string `json:"2008"`
				Num2009 string `json:"2009"`
				Num2010 string `json:"2010"`
				Num2011 string `json:"2011"`
				Num2012 string `json:"2012"`
				Num2013 string `json:"2013"`
				Num2014 string `json:"2014"`
				Num2015 string `json:"2015"`
				Num2016 string `json:"2016"`
				Num2017 string `json:"2017"`
				Num2018 string `json:"2018"`
				Num2019 string `json:"2019"`
				Num2020 string `json:"2020"`
				Num2021 string `json:"2021"`
				Num2022 string `json:"2022"`
				Num2023 string `json:"2023"`
				Num2024 string `json:"2024"`
			} `json:"serie"`
		} `json:"series"`
	} `json:"resultados"`
}

type DataPerCapta struct {
	Ano   string  `json:"ano" csv:"ano"`
	Valor float64 `json:"valor" csv:"valor"`
}

type PIBPerCapta struct {
	Atualizacao   time.Time      `json:"data_atualizacao"`
	UnidadeMedida string         `json:"unidade_medida"`
	Fonte         string         `json:"fonte"`
	Data          []DataPerCapta `json:"data"`
}

func RunnerPIBPerCapta() {
	runnerName := "PIB - Per Capta"
	url := "https://servicodados.ibge.gov.br/api/v3/agregados/6784/periodos/-48/variaveis/9812?classificacao=&localidades=N1"
	unidadeMedida := "PIB Per Capta"
	fonte := "https://servicodados.ibge.gov.br"
	file_path := "./data/pib/pib-per-capta.json"
	fileNameOutputCSV := "./data/pib/pib-per-capta.csv"

	s3KeyCSV := "pib/pib-per-capta.csv"
	s3KeyJSON := "pib/pib-per-capta.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	pib := &PIBPerCapta{}

	l.Info().
		Str("Runner", runnerName).
		Msg("Atualizando campo da data/hora da atualização dos dados")

	now := time.Now()
	pib.Atualizacao = now
	pib.Fonte = fonte
	pib.UnidadeMedida = unidadeMedida

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Realizando o request na API do IBGE para recuperar a série histórica do pib")

	res, err := http.Get(url)
	defer res.Body.Close()

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("URL", url).
			Msg("Erro ao realizar o request HTTP para o endpoint dos dados")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("URL", url).
		Msg("Request finalizado com sucesso")

	l.Info().
		Str("Runner", runnerName).
		Msg("Realizando o decode do JSON na Struct de Response")

	var response ResponsePerCapta

	decoder := json.NewDecoder(res.Body)
	err = decoder.Decode(&response)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Msg("Erro ao converter o response JSON na Struct Response")
	}

	// Recupera o nome dos campos da Struct onde ficam os valores do PIB
	names := structs.Names(response[0].Resultados[0].Series[0].Serie)

	for _, v := range names {

		r := reflect.ValueOf(response[0].Resultados[0].Series[0].Serie)
		f := reflect.Indirect(r).FieldByName(v)

		valueRaw := f.String()

		l.Info().
			Str("Runner", runnerName).
			Str("Valor recuperado", valueRaw).
			Msg("Convertendo valor recuperado da pib para Float64")

		if valueRaw == "" {
			l.Warn().
				Str("Runner", runnerName).
				Str("Valor recuperado", valueRaw).
				Msg("Ignorando o valor recuperado pois inda não foi contabilizado pela fonte")
			continue
		}

		valor, err := strconv.ParseFloat(strings.TrimSpace(valueRaw), 64)

		if err != nil {
			l.Fatal().
				Str("Runner", runnerName).
				Str("Error", err.Error()).
				Str("Valor recuperado", valueRaw).
				Msg("Erro ao converter o valor para Float64")
		}

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Msg("Removendo patterns desnecessários da string")

		ano := strings.Replace(v, "Num", "", -1)

		l.Info().
			Str("Runner", runnerName).
			Str("Data recuperada", v).
			Str("Data sem os patterns removidos", ano).
			Msg("Patterns removidos da string")

		item := DataPerCapta{
			Ano:   ano,
			Valor: valor,
		}

		pib.Data = append(pib.Data, item)
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(pib)
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

	csvOutput, err := gocsv.MarshalString(&pib.Data)
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
