package ambientais

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	"github.com/gocarina/gocsv"
)

type DataPoluentesAtmosfericosAnualConsolidado struct {
	Referencia       string  `json:"referencia" csv:"referencia"`
	SaoPaulo         float64 `json:"sao_paulo" csv:"sao_paulo"`
	MatoGrossoSul    float64 `json:"mato_grosso_do_sul" csv:"mato_grosso_do_sul"`
	Rondonia         float64 `json:"rondonia" csv:"rondonia"`
	Amazonas         float64 `json:"amazonas" csv:"amazonas"`
	Pernambuco       float64 `json:"pernambuco" csv:"pernambuco"`
	Ceara            float64 `json:"ceara" csv:"ceara"`
	Para             float64 `json:"para" csv:"para"`
	Tocantins        float64 `json:"tocantins" csv:"tocantins"`
	Sergipe          float64 `json:"sergipe" csv:"sergipe"`
	MinasGerais      float64 `json:"minas_gerais" csv:"minas_gerais"`
	RioDeJaneiro     float64 `json:"rio_de_janeiro" csv:"rio_de_janeiro"`
	Goias            float64 `json:"goias" csv:"goias"`
	RioGrandeDoSul   float64 `json:"rio_grande_do_sul" csv:"rio_grande_do_sul"`
	EspiritoSanto    float64 `json:"espirito_santo" csv:"espirito_santo"`
	Bahia            float64 `json:"bahia" csv:"bahia"`
	Alagoas          float64 `json:"alagoas" csv:"alagoas"`
	DistritoFederal  float64 `json:"distrito_federal" csv:"distrito_federal"`
	Acre             float64 `json:"acre" csv:"acre"`
	RioGrandeDoNorte float64 `json:"rio_grande_do_norte" csv:"rio_grande_do_norte"`
	Maranhao         float64 `json:"maranhao" csv:"maranhao"`
	Parana           float64 `json:"parana" csv:"parana"`
	Paraiba          float64 `json:"paraiba" csv:"paraiba"`
	SantaCatarina    float64 `json:"santa_catarina" csv:"santa_catarina"`
	Piaui            float64 `json:"piaui" csv:"piaui"`
	Amapa            float64 `json:"amapa" csv:"amapa"`
	Total            float64 `json:"total" csv:"total"`
}

type PoluentesAtmosfericosAnualConsolidado struct {
	Atualizacao time.Time                                   `json:"data_atualizacao"`
	Fonte       string                                      `json:"fonte"`
	Data        []DataPoluentesAtmosfericosAnualConsolidado `json:"data"`
}

func RunnerPoluentesAtmosfericosAnualConsolidado(emissores PoluentesAtmosfericosEmpresas) {
	runnerName := "Poluentes Atmosféricos - Anual Consolidado"

	l := logger.Instance()

	index := PoluentesAtmosfericosAnualConsolidado{}
	now := time.Now()
	index.Atualizacao = now
	index.Fonte = emissores.Fonte

	file_path := "./data/ambientais/emissao_poluentes_anual_consolidado.json"
	fileNameOutputCSV := "./data/ambientais/emissao_poluentes_anual_consolidado.csv"

	s3KeyCSV := "ambientais/emissao_poluentes_anual_consolidado.csv"
	s3KeyJSON := "ambientais/emissao_poluentes_anual_consolidado.csv"

	acc := make(map[string]DataPoluentesAtmosfericosAnualConsolidado)

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	l.Info().
		Str("Runner", runnerName).
		Msg("Gerando os Indices")

	for _, i := range emissores.Data {
		item := DataPoluentesAtmosfericosAnualConsolidado{}
		item.Referencia = i.Referencia
		acc[item.Referencia] = item
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Somando os indicadores anuais por estado")

	for _, i := range emissores.Data {
		item := acc[i.Referencia]

		item.Total += i.Quantidade

		if i.Estado == "SAO PAULO" {
			item.SaoPaulo += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "MATO GROSSO DO SUL" {
			item.MatoGrossoSul += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "RONDONIA" {
			item.Rondonia += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "AMAZONAS" {
			item.Amazonas += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "PERNAMBUCO" {
			item.Pernambuco += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "CEARA" {
			item.Ceara += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "PARA" {
			item.Para += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "TOCANTINS" {
			item.Tocantins += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "SANTA CATARINA" {
			item.SantaCatarina += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "MINAS GERAIS" {
			item.MinasGerais += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "AMAPA" {
			item.Amapa += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "RIO DE JANEIRO" {
			item.RioDeJaneiro += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "GOIAS" {
			item.Goias += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "PARAIBA" {
			item.Paraiba += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "RIO GRANDE DO SUL" {
			item.RioGrandeDoSul += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "RIO GRANDE DO NORTE" {
			item.RioGrandeDoNorte += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "ESPIRITO SANTO" {
			item.EspiritoSanto += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "BAHIA" {
			item.Bahia += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "ALAGOAS" {
			item.Alagoas += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "DISTRITO FEDERAL" {
			item.DistritoFederal += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "ACRE" {
			item.Acre += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "MARANHAO" {
			item.Maranhao += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "PARANA" {
			item.Parana += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "PIAUI" {
			item.Piaui += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

		if i.Estado == "SERGIPE" {
			item.Sergipe += i.Quantidade
			acc[item.Referencia] = item
			continue
		}

	}

	for _, i := range emissores.Data {
		item := acc[i.Referencia]
		item.Total = math.Ceil(item.Total*100) / 100
		item.SaoPaulo = math.Ceil(item.SaoPaulo*100) / 100
		item.RioDeJaneiro = math.Ceil(item.RioDeJaneiro*100) / 100
		item.Piaui = math.Ceil(item.Piaui*100) / 100
		item.Para = math.Ceil(item.Para*100) / 100
		item.Paraiba = math.Ceil(item.Paraiba*100) / 100
		item.Parana = math.Ceil(item.Parana*100) / 100
		item.Maranhao = math.Ceil(item.Maranhao*100) / 100
		item.Acre = math.Ceil(item.Acre*100) / 100
		item.DistritoFederal = math.Ceil(item.DistritoFederal*100) / 100
		item.Alagoas = math.Ceil(item.Alagoas*100) / 100
		item.RioGrandeDoNorte = math.Ceil(item.RioGrandeDoNorte*100) / 100
		item.RioGrandeDoSul = math.Ceil(item.RioGrandeDoSul*100) / 100
		item.MatoGrossoSul = math.Ceil(item.MatoGrossoSul*100) / 100
		item.Rondonia = math.Ceil(item.Rondonia*100) / 100
		item.Pernambuco = math.Ceil(item.Pernambuco*100) / 100
		item.Ceara = math.Ceil(item.Ceara*100) / 100
		item.Amazonas = math.Ceil(item.Amazonas*100) / 100
		item.MinasGerais = math.Ceil(item.MinasGerais*100) / 100
		item.Goias = math.Ceil(item.Goias*100) / 100
		item.Tocantins = math.Ceil(item.Tocantins*100) / 100
		item.EspiritoSanto = math.Ceil(item.EspiritoSanto*100) / 100
		item.Bahia = math.Ceil(item.Bahia*100) / 100
		item.SantaCatarina = math.Ceil(item.SantaCatarina*100) / 100
		item.Amapa = math.Ceil(item.Amapa*100) / 100
		item.Sergipe = math.Ceil(item.Sergipe*100) / 100
		acc[item.Referencia] = item
	}

	fmt.Println(acc)

	for _, i := range acc {
		index.Data = append(index.Data, i)
	}

	// Sort do data
	sort.Slice(index.Data, func(i, j int) bool {
		return index.Data[i].Referencia < index.Data[j].Referencia
	})

	l.Info().
		Str("Runner", runnerName).
		Msg("Convertendo a Struct do Schema em formato JSON")

	b, err := json.Marshal(index)
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

	e := os.Remove(fileNameOutputCSV)
	if e != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao remover o CSV")
	}

	csvFile, err := os.OpenFile(fileNameOutputCSV, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("FilePath", fileNameOutputCSV).
			Str("Error", err.Error()).
			Msg("Erro ao criar o dataset em CSV")
	}
	defer csvFile.Close()

	csvOutput, err := gocsv.MarshalString(&index.Data)
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
