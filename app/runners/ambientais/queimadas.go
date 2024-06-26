package ambientais

import (
	"crawlers/pkg/logger"
	"crawlers/pkg/upload"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gocarina/gocsv"
)

type DataQueimadas struct {
	Referencia  string `json:"referencia" csv:"referencia"`
	Ano         string `json:"ano" csv:"ano"`
	Mes         string `json:"mes" csv:"mes"`
	Anomes      string `json:"ano_mes" csv:"ano_mes"`
	Focos       int64  `json:"focos_incendio" csv:"focos_incendio"`
	Acumulado   int64  `json:"acumulado_ano" csv:"acumulado_ano"`
	Consolidado bool   `json:"consolidado" csv:"consolidado"`
}

type Queimadas struct {
	Atualizacao   time.Time       `json:"data_atualizacao"`
	Fonte         string          `json:"fonte"`
	UnidadeMedida string          `json:"unidade_medida"`
	Data          []DataQueimadas `json:"data"`
}

type QueimadasCSV struct {
	Ano       string  `csv:"Ano"`
	Janeiro   float64 `csv:"Janeiro"`
	Fevereiro float64 `csv:"Fevereiro"`
	Marco     float64 `csv:"Março"`
	Abril     float64 `csv:"Abril"`
	Maio      float64 `csv:"Maio"`
	Junho     float64 `csv:"Junho"`
	Julho     float64 `csv:"Julho"`
	Agosto    float64 `csv:"Agosto"`
	Setembro  float64 `csv:"Setembro"`
	Outubro   float64 `csv:"Outubro"`
	Novembro  float64 `csv:"Novembro"`
	Dezembro  float64 `csv:"Dezembro"`
	Total     float64 `csv:"Total"`
}

func RunnerQueimadas() {
	runnerName := "Queimadas - INPE"
	domain := "queimadas.dgi.inpe.br"
	url := "https://terrabrasilis.dpi.inpe.br/queimadas/situacao-atual/media/csv_estatisticas/historico_pais_brasil.csv"

	fileNameRaw := "./data/ambientais/raw/historico_pais_brasil.csv"

	file_path := "./data/ambientais/queimadas.json"
	fileNameOutputCSV := "./data/ambientais/queimadas.csv"

	s3KeyCSV := "ambientais/queimadas.csv"
	s3KeyJSON := "ambientais/queimadas.json"

	now := time.Now()
	indice := &Queimadas{}
	indice.Fonte = domain
	indice.Atualizacao = now

	l := logger.Instance()

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Criando o arquivo de recepção temporario")

	f, err := os.Create(fileNameRaw)
	defer f.Close()

	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("Erro", err.Error()).
			Msg("Falha ao criar o arquivo temporario")
	}

	client := http.Client{
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			r.URL.Opaque = r.URL.Path
			return nil
		},
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Str("URL", url).
		Msg("Realizando o download do arquivo")

	resp, err := client.Get(url)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", url).
			Str("Erro", err.Error()).
			Msg("Erro ao fazer o request HTTP para a URL selecionada")
	}
	defer resp.Body.Close()

	size, err := io.Copy(f, resp.Body)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", url).
			Str("Erro", err.Error()).
			Msg("Erro ao escrever no arquivo temporario")
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Str("URL", url).
		Int64("Size", size).
		Msg("Escrita no arquivo temporário concluído")

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("Lendo o arquivo temporário")

	tmpFile, err := os.OpenFile(fileNameRaw, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("Error", err.Error()).
			Msg("Erro ao ler o arquivo temporário")
		panic(err)
	} else {
		l.Info().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Msg("Arquivo temporário lido")
	}
	defer tmpFile.Close()

	// O arquivo mudou o formato e agora a variável "Ano" está em branco. Foi necessário reescrever a primeira linha do CSV para os headers novos

	// Substituir a primeira linha do arquivo.

	// Ler o conteúdo do arquivo
	conteudoArquivo, err := ioutil.ReadFile(fileNameRaw)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("Error", err.Error()).
			Msg("Erro ao ler o arquivo")
		return
	}

	// Converter o conteúdo do arquivo em um slice de strings dividido por linhas
	linhas := strings.Split(string(conteudoArquivo), "\n")

	// Modificar a primeira linha (índice 0) como desejado
	novaPrimeiraLinha := "Ano,Janeiro,Fevereiro,Março,Abril,Maio,Junho,Julho,Agosto,Setembro,Outubro,Novembro,Dezembro,Total"

	if len(linhas) > 0 {
		linhas[0] = novaPrimeiraLinha
	}

	// Juntar as linhas de volta em uma única string
	novoConteudo := strings.Join(linhas, "\n")

	// Escrever o conteúdo modificado de volta no arquivo
	err = ioutil.WriteFile(fileNameRaw, []byte(novoConteudo), os.ModePerm)
	if err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("Error", err.Error()).
			Msg("Erro ao escrever o arquivo")
		return
	}

	l.Info().
		Str("Runner", runnerName).
		Str("FilePath", fileNameRaw).
		Msg("A primeira linha foi substituída com sucesso.")

	queimadasCSV := []*QueimadasCSV{}

	if err := gocsv.UnmarshalFile(tmpFile, &queimadasCSV); err != nil {
		l.Error().
			Str("Runner", runnerName).
			Str("FilePath", fileNameRaw).
			Str("URL", url).
			Str("Erro", err.Error()).
			Msg("Erro ao converter o CSV em Struct")
		panic(err)
	}

	acc := []DataQueimadas{}

	for _, periodo := range queimadasCSV {

		// Removendo as colunas que não contem as informacões historicas
		ano_int, _ := strconv.ParseInt(strings.TrimSpace(periodo.Ano), 10, 64)
		if ano_int == 0 {
			continue
		}

		// Janeiro
		janeiro := fmt.Sprintf("%s-01", periodo.Ano)
		janeiro_vl := periodo.Janeiro
		item_01 := &DataQueimadas{
			Referencia: janeiro,
			Focos:      int64(janeiro_vl),
			Ano:        periodo.Ano,
			Mes:        "01",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "01"),
		}

		if janeiro_vl != 0 {
			acc = append(acc, *item_01)
		}

		// Fevereiro
		fevereiro := fmt.Sprintf("%s-02", periodo.Ano)
		fevereiro_vl := periodo.Fevereiro
		item_02 := &DataQueimadas{
			Referencia: fevereiro,
			Focos:      int64(fevereiro_vl),
			Ano:        periodo.Ano,
			Mes:        "02",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "02"),
		}

		if fevereiro_vl != 0 {
			acc = append(acc, *item_02)
		}

		//Março
		marco := fmt.Sprintf("%s-03", periodo.Ano)
		marco_vl := periodo.Marco
		item_03 := &DataQueimadas{
			Referencia: marco,
			Focos:      int64(marco_vl),
			Ano:        periodo.Ano,
			Mes:        "03",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "03"),
		}

		if marco_vl != 0 {
			acc = append(acc, *item_03)
		}

		// Abril
		abril := fmt.Sprintf("%s-04", periodo.Ano)
		abril_vl := periodo.Abril
		item_04 := &DataQueimadas{
			Referencia: abril,
			Focos:      int64(abril_vl),
			Ano:        periodo.Ano,
			Mes:        "04",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "04"),
		}

		if abril_vl != 0 {
			acc = append(acc, *item_04)
		}

		// Maio
		maio := fmt.Sprintf("%s-05", periodo.Ano)
		maio_vl := periodo.Maio
		item_05 := &DataQueimadas{
			Referencia: maio,
			Focos:      int64(maio_vl),
			Ano:        periodo.Ano,
			Mes:        "05",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "05"),
		}

		if maio_vl != 0 {
			acc = append(acc, *item_05)
		}

		// Junho
		junho := fmt.Sprintf("%s-06", periodo.Ano)
		junho_vl := periodo.Junho
		item_06 := &DataQueimadas{
			Referencia: junho,
			Focos:      int64(junho_vl),
			Ano:        periodo.Ano,
			Mes:        "06",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "06"),
		}

		if junho_vl != 0 {
			acc = append(acc, *item_06)
		}

		// Julho
		julho := fmt.Sprintf("%s-07", periodo.Ano)
		julho_vl := periodo.Julho
		item_07 := &DataQueimadas{
			Referencia: julho,
			Focos:      int64(julho_vl),
			Ano:        periodo.Ano,
			Mes:        "07",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "07"),
		}

		if julho_vl != 0 {
			acc = append(acc, *item_07)
		}

		// Agosto
		agosto := fmt.Sprintf("%s-08", periodo.Ano)
		agosto_vl := periodo.Agosto
		item_08 := &DataQueimadas{
			Referencia: agosto,
			Focos:      int64(agosto_vl),
			Ano:        periodo.Ano,
			Mes:        "08",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "08"),
		}

		if agosto_vl != 0 {
			acc = append(acc, *item_08)
		}

		// Setembro
		setembro := fmt.Sprintf("%s-09", periodo.Ano)
		setembro_vl := periodo.Setembro
		item_09 := &DataQueimadas{
			Referencia: setembro,
			Focos:      int64(setembro_vl),
			Ano:        periodo.Ano,
			Mes:        "09",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "09"),
		}

		if setembro_vl != 0 {
			acc = append(acc, *item_09)
		}

		// Outubro
		outubro := fmt.Sprintf("%s-10", periodo.Ano)
		outubro_vl := periodo.Outubro
		item_10 := &DataQueimadas{
			Referencia: outubro,
			Focos:      int64(outubro_vl),
			Ano:        periodo.Ano,
			Mes:        "10",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "10"),
		}

		if outubro_vl != 0 {
			acc = append(acc, *item_10)
		}

		// Novembro
		novembro := fmt.Sprintf("%s-11", periodo.Ano)
		novembro_vl := periodo.Novembro
		item_11 := &DataQueimadas{
			Referencia: novembro,
			Focos:      int64(novembro_vl),
			Ano:        periodo.Ano,
			Mes:        "11",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "11"),
		}

		if novembro_vl != 0 {
			acc = append(acc, *item_11)
		}

		// Dezembro
		dezembro := fmt.Sprintf("%s-12", periodo.Ano)
		dezembro_vl := periodo.Dezembro
		item_12 := &DataQueimadas{
			Referencia: dezembro,
			Focos:      int64(dezembro_vl),
			Ano:        periodo.Ano,
			Mes:        "12",
			Anomes:     fmt.Sprintf("%s%s", periodo.Ano, "23"),
		}
		if dezembro_vl != 0 {
			acc = append(acc, *item_12)
		}

	}

	for i, k := range acc {
		if k.Mes == "01" || i == 0 {
			k.Acumulado = k.Focos
		} else {
			l := indice.Data[len(indice.Data)-1]
			k.Acumulado = l.Acumulado + k.Focos
		}

		if k.Mes == "12" {
			k.Consolidado = true
		}

		if k.Focos != 0 {
			indice.Data = append(indice.Data, k)
		}
	}

	// indice.Data[len(indice.Data)-1].Consolidado = true

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

	f, err = os.Create(file_path)
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
