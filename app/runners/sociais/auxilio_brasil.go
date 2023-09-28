package sociais

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/gocolly/colly"
)

type DataAuxilioBrasilConsolidado struct {
	Referencia              string  `json:"referencia" csv:"referencia"`
	AnoMes                  int64   `json:"ano_mes" csv:"ano_mes"`
	Familias                int64   `json:"familias_cobertura" csv:"familias_cobertura"`
	FamiliasVulnerabilidade int64   `json:"familias_vulneraveis" csv:"familias_vulneraveis"`
	CoberturaAuxilio        float64 `json:"cobertura_auxilio" csv:"cobertura_auxilio"`
	ValorTotalRepassado     float64 `json:"valor_total_repassado" csv:"valor_total_repassado"`
	ValorBeneficio          float64 `json:"valor_beneficio" csv:"valor_beneficio"`
}

type AuxilioBrasilConsolidado struct {
	Atualizacao   time.Time                      `json:"data_atualizacao"`
	Fonte         string                         `json:"fonte"`
	UnidadeMedida string                         `json:"unidade_medida"`
	Data          []DataAuxilioBrasilConsolidado `json:"data"`
}

type DataAuxilioBrasil struct {
	Referencia          string  `json:"referencia" csv:"referencia"`
	Familias            int64   `json:"familias_cobertura" csv:"familias_cobertura"`
	ValorTotalRepassado float64 `json:"valor_total_repassado" csv:"valor_total_repassado"`
	ValorBeneficio      float64 `json:"valor_beneficio" csv:"valor_beneficio"`
}

type AuxilioBrasil struct {
	Atualizacao   time.Time           `json:"data_atualizacao"`
	Fonte         string              `json:"fonte"`
	UnidadeMedida string              `json:"unidade_medida"`
	Data          []DataAuxilioBrasil `json:"data"`
}

func RunnerAuxilioBrasil() {

	runnerName := "Auxilio Brasil"
	domain := "aplicacoes.cidadania.gov.br"
	url := "https://aplicacoes.cidadania.gov.br/vis/data3/v.php?q%5B%5D=oNOhlMHqwJOsuqSe9Wp%2BhrNe09Gv17llja1%2BYW15YmqqdH9%2BaV%2BEkmSXbWTZ8X5kc3xwoNqlwLNyocnWmKV4mb7nwJl3g6iv5lzDf2lfiJyZy6mmwrazlai7mnW0n666qpKSnKbfqlbTrH1rcYOprO6eiMKporycbtCen9DgiG%2BvvaJd9Fqwr6qSd9ibz6tTnfF%2BZm55c2qZrbWzpU3J0KjYoVuFu8NlbH9qdLOnwrucn8DEXJl9qY6tf2Voel5a3qXAs1ebzM2fiqKhwZzKb6Kpoa3edLOvo6C8nG7Qnp%2FQ4Ihvr72itr%2BauhHkmcDCpo2DlMo%2B%2BqClqahayXqPbqXw%2BtBT3bKmzeC7p527WJDapbzAV4HG1ZTWXYXC666nr6mZqZmdvG54os8k4Namon29v5WvsaFdu567s53wBMSc2V2gICSxnatop5%2Fcnq%2B3m5x30ZjWnqZ94a6h%2F%2FWho9qsbbKmTZjWqy3qn8bqbXauqaij5bW9iQ%3D%3D"
	unidadeMedida := "Cobertura do Auxilio Brasil"
	fonte := "https://aplicacoes.cidadania.gov.br"
	file_path := "./data/sociais/auxilio_brasil.json"
	fileNameOutputCSV := "./data/sociais/auxilio_brasil.csv"

	s3KeyCSV := "sociais/auxilio_brasil.csv"
	s3KeyJSON := "sociais/auxilio_brasil.json"

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Runner para Efetuar o Crawler")

	c := colly.NewCollector(
		colly.AllowedDomains(domain),
	)

	indice := &AuxilioBrasil{}

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
			familias_td := strings.Replace(tr.ChildText("td:nth-child(2)"), ".", "", -1)

			valor_total_repassado_td := strings.Replace(tr.ChildText("td:nth-child(3)"), ".", "", -1)
			valor_total_repassado_td = strings.Replace(valor_total_repassado_td, ",", ".", -1)
			valor_total_repassado_td = strings.Replace(valor_total_repassado_td, "R$ ", "", -1)

			valor_beneficio_td := strings.Replace(tr.ChildText("td:nth-child(4)"), ".", "", -1)
			valor_beneficio_td = strings.Replace(valor_beneficio_td, ",", ".", -1)
			valor_beneficio_td = strings.Replace(valor_beneficio_td, "R$ ", "", -1)

			l.Info().
				Str("Runner", runnerName).
				Str("Domain", domain).
				Str("Referencia", referencia_td).
				Str("Valor Total Repassado", valor_total_repassado_td).
				Str("Valor Beneficio", valor_beneficio_td).
				Str("Referencia", referencia_td).
				Msg("Adicionando item ao dataset")

			familias, _ := strconv.ParseInt(strings.TrimSpace(familias_td), 10, 64)
			valor_total_repassado, _ := strconv.ParseFloat(strings.TrimSpace(valor_total_repassado_td), 64)
			valor_beneficio, _ := strconv.ParseFloat(strings.TrimSpace(valor_beneficio_td), 64)

			item := DataAuxilioBrasil{
				Referencia:          referencia_td,
				Familias:            familias,
				ValorTotalRepassado: valor_total_repassado,
				ValorBeneficio:      valor_beneficio,
			}

			if item.Familias > 0 {
				l.Info().
					Str("Runner", runnerName).
					Str("Domain", domain).
					Str("Referencia", referencia_td).
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

func RunnerAuxilioBrasilConsolidacao() {
	l := logger.Instance()

	consolidado := make(map[string]DataAuxilioBrasilConsolidado)

	runnerName := "Consolidação Auxilio Emergencial"
	fonte := "https://aplicacoes.cidadania.gov.br"

	indices_pobreza_file := "./data/sociais/indices_pobreza_consolidado.json"
	auxilio_brasil_file := "./data/sociais/auxilio_brasil.json"

	file_path := "./data/sociais/auxilio_brasil_consolido.json"
	fileNameOutputCSV := "./data/sociais/auxilio_brasil_consolido.csv"

	s3KeyCSV := "sociais/auxilio_brasil_consolido.csv"
	s3KeyJSON := "sociais/auxilio_brasil_consolido.json"

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Processo de Consolidação de dados")

	l.Info().
		Str("Runner", runnerName).
		Msg("Iniciando o Processo de Consolidação de dados")

	indice := AuxilioBrasilConsolidado{}
	now := time.Now()
	indice.Atualizacao = now
	indice.Fonte = fonte

	// Auxilio Brasil
	dataAuxilioBrasil := AuxilioBrasil{}
	fileAuxilioBrasil, err := ioutil.ReadFile(auxilio_brasil_file)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", auxilio_brasil_file).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(fileAuxilioBrasil), &dataAuxilioBrasil)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", auxilio_brasil_file).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataAuxilioBrasil.Data {

		ano := k.Referencia[3:7]
		mes := k.Referencia[0:2]

		anomes_str := fmt.Sprintf("%v%v", ano, mes)
		anomes, _ := strconv.ParseInt(anomes_str, 10, 64)

		item := DataAuxilioBrasilConsolidado{
			Referencia:          k.Referencia,
			Familias:            k.Familias,
			ValorTotalRepassado: k.ValorTotalRepassado,
			ValorBeneficio:      k.ValorBeneficio,
			AnoMes:              anomes,
		}

		consolidado[k.Referencia] = item
	}

	// Data da População
	dataPobreza := PobrezaConsolidado{}
	filePobreza, err := ioutil.ReadFile(indices_pobreza_file)

	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", indices_pobreza_file).
			Msg("Erro ao ler o arquivo")
	}

	err = json.Unmarshal([]byte(filePobreza), &dataPobreza)
	if err != nil {
		l.Fatal().
			Str("Runner", runnerName).
			Str("Error", err.Error()).
			Str("Arquivo", indices_pobreza_file).
			Msg("converter para struct")
	}

	l.Info().
		Str("Runner", runnerName).
		Msg("Adicionando os Itens do Dataset de Extrema Pobreza para o Dataset Consolidado")

	for _, k := range dataPobreza.Data {
		item := consolidado[k.Referencia]
		if item.AnoMes != 0 {
			item.FamiliasVulnerabilidade = k.FamiliasVulnerabilidade
			item.CoberturaAuxilio = float64(item.Familias) / float64(item.FamiliasVulnerabilidade)
			consolidado[k.Referencia] = item
		}
	}
	// Criando o objeto final

	for _, i := range consolidado {
		if i.FamiliasVulnerabilidade != 0 {
			indice.Data = append(indice.Data, i)
		}
	}

	// Sort do data
	sort.Slice(indice.Data, func(i, j int) bool {
		return indice.Data[i].AnoMes < indice.Data[j].AnoMes
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
