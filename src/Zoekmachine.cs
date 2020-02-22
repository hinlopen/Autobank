using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SQLite;

namespace DAR1
{
public static class Zoekmachine
{
static string[] attributen  = new string[] {"mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type" };
static string[] numerisch   = new string[] {"mpg", "displacement", "horsepower", "weight", "acceleration", "model_year" };
static string[] categorisch = new string[] {"cylinders", "origin", "brand", "model", "type" };

static string res_pad = "../../../res/";
static string db_naam = res_pad + "autobank.sqlite";
static string verbinding_tekst = "Data Source=" + db_naam + ";Version=3;";
static int N;

static Dictionary<string, string> C;
static Dictionary<string, List<string>> C_in;

static Dictionary<string, double> IDF, H, QF;
static Dictionary<string, Dictionary<string, double>> Jac;

static Dictionary<string, string> ZTCat;
static Dictionary<string, double> ZTNum;

private static void InitTabellen()
{
    N = Autobank.tel_tupels("autompg");
    IDF = new Dictionary<string, double>();
    H   = new Dictionary<string, double>();
    QF  = new Dictionary<string, double>(); //TODO, gebruiken?
    Jac = new Dictionary<string, Dictionary<string, double>>(); //TODO, gebruiken?

    C_in = new Dictionary<string, List<string>>();

    ZTCat = new Dictionary<string, string>();
    ZTNum = new Dictionary<string, double>();

    foreach(string a in C.Keys)
    {
        if (numerisch.Contains(a)) ZTNum[a] = double.Parse(C[a]);
        else                       ZTCat[a] = C[a];
    }

    foreach(var a in C.Keys)
    {
        IDF[a] = get_IDF(a, C[a]);
        QF[a]  = get_QF(a, C[a]);
        if (numerisch.Contains(a)) H[a] = get_H(a);
    }
}

public static Tuple<int,double>[] VerwerkQuery(Dictionary<string, string> xs, int k)
{
    C = xs;
    InitTabellen();

    var oorspronkelijke_termen = new Dictionary<string, string>(C);
    var gesorteerd = Autobank.populairste_attributen(C.Keys.ToList());
    string sql = maak_sql_query();
    int aantal_resultaten = Autobank.tel_tupels("autompg", sql);

    int r = 3;
    while (r < 10 && C.Count > 0)
    {
        if (aantal_resultaten >= k)
            break;

        //versoepel query, begin met attribuut met hoogste RQF
        foreach (string a in gesorteerd)
        {
            if (numerisch.Contains(a)) continue;

            C_in[a] = versoepel_zoekopdracht(a, r);
            C.Remove(a); //todo kan fout gaan?

            sql = maak_sql_query();
            aantal_resultaten = Autobank.tel_tupels("autompg", sql);
            if (aantal_resultaten >= k)
                break;
        }

        r++;
    }

    foreach(var a in C_in.Keys)
    {
        Jac[a] = new Dictionary<string, double>();
        foreach (var s in C_in[a])
        {
            Jac[a][s] = get_Jac(a, oorspronkelijke_termen[a], s);
        }
    }

    var Scores = BerekenScores(sql, aantal_resultaten);

    // if aantal = k, alleen sorteren.
    return TopK(Scores, C.Keys.ToArray(), k);
}

private static List<string> versoepel_zoekopdracht(string a, int soepelheid)
{
    var res = new List<string>();
    if (numerisch.Contains(C[a]))
    {

    }
    else
    {
        var l = Autobank.vergelijkbare_attributen(a, C[a]);

        int lim = Math.Min(soepelheid, l.Count);

        res.Add(C[a]);
        for (int i = 1; i < lim; i++)
        {
            var e = l[i].Item1;
            if (res[0] == e) continue;
            res.Add(e);
        }
    }
    return res;
}

private static string maak_sql_query()
{
    StringBuilder sb = new StringBuilder();
    sb.Append(" WHERE ");

    int i = 0;
    foreach( string a in C.Keys)
    {
        sb.Append(String.Format("{0} = '{1}' {2}", a, C[a], i < (C.Count-1) ? " AND " : ""));
        i++;
    }

    i = C_in.Count-1;
    if (C_in.Keys.Count > 0 && C.Keys.Count > 0) sb.Append(" AND ");
    foreach( string a in C_in.Keys)
    {
        sb.Append(a + " IN (");


        int j = C_in[a].Count-1;
        foreach(string l in C_in[a])
        {
            sb.Append(String.Format("'{0}'", l));
            sb.Append(j > 0 ? ", " : ""); 
            j--;
        }

        sb.Append(")");
        sb.Append(i > 0 ? " AND " : "");
        i--;
    }

    sb.Append(";");

    return sb.ToString();
}

private static double[] BerekenScores(string sql, int aantal_tupels)
{
    var Scores = new double[aantal_tupels*(C.Count+C_in.Count)];

    var score_map = Dictionary<string, List<Tuple<int,double>>>();

    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();

        SQLiteCommand command = new SQLiteCommand("SELECT * FROM autompg " + sql.TrimEnd(new char[] {';'}) + " ORDER BY id;", sc);
        SQLiteDataReader Tupel = command.ExecuteReader();
        int i = 0;
        while(Tupel.Read())
        {
            int tid = int.Parse(Tupel["id"].ToString());
            foreach(string a in ZTCat.Keys)
            {
                score_map[a].Add(Tuple.Create(tid, (Tupel[a].ToString() == ZTCat[a]) ? IDF[a] : 0))
                Scores[i++] = (Tupel[a].ToString() == ZTCat[a]) ? IDF[a] : 0;
            }

            foreach(string a in ZTNum.Keys)
            {
                double t = double.Parse(Tupel[a].ToString());
                double d = (t - ZTNum[a]) / H[a];
                Scores[i++] = Math.Pow(Math.E, -0.5 * (d * d) * IDF[a]);
            }
            
            foreach(string a in C_in.Keys)
            {
                string Tk = Tupel[a].ToString();
                if (C_in[a].Contains(Tk))
                    Scores[i++] = ( true) ? (QF[a] * IDF[a] + 1) : QF[a] * Jac[a][Tk];   
            }
        }
        sc.Close();
    }

    return Scores;
}

public static Tuple<int,double>[] TopK(double[] Scores, string[] zoek_attributen, int k)
{
    //We kopieren de id-score tabellen voor elk attribuut, en sorteren de een op id, de ander op score voor indexering.
    Dictionary<string, List<Tuple<int, double>>> tid_map, waarde_map;
    waarde_map = new Dictionary<string, List<Tuple<int,double>>>();
    tid_map    = new Dictionary<string, List<Tuple<int,double>>>();

    int m = zoek_attributen.Length;

    int j = 0;
    foreach (string a in zoek_attributen)
    {
        tid_map[a]    = new List<Tuple<int,double>>(N+1);
        waarde_map[a] = new List<Tuple<int,double>>(N+1);
        tid_map[a].Add(Tuple.Create(0,-1.0)); //marge zodat ids goed lopen
        waarde_map[a].Add(Tuple.Create(0,-1.0));
        for (int i = 0; i < N; i++)
        {
            var t = Tuple.Create(i+1, Scores[i*m + j]);
            tid_map[a].Add(t);
            waarde_map[a].Add(t);
        }

        j++;
        waarde_map[a].Sort((x,y) => y.Item2.CompareTo(x.Item2)); //sorteer deze lijst zodra alle scores erin zitten
    }

    // tid_map heeft voor elk attribuut een lijst met op index i de waarde van tupel_id i!
    // Als we dus een tupel_id hebben, kunnen we snel elke waarde terugvinden!
    // waarde_map heeft lijsten met op index i het op i-1 na hoogste score, zodat TA snel kan termineren!

    Dictionary<int, double> buffer = new Dictionary<int, double>();
    List<Tuple<int, double>> topK  = new List<Tuple<int, double>>(k);
    Dictionary<string, double> max = new Dictionary<string, double>(waarde_map.Count);
    double drempel;

    HashSet<int> gezien = new HashSet<int>();

    int r = 0;
    while (topK.Count < k || r > waarde_map[zoek_attributen[0]].Count)
    {
        drempel = 0;

        foreach(var a in waarde_map.Keys)
        {
            // De maximale score (som van attr.) die nog behaald kan worden
            // Alles wat we vinden dat hierboven zit, gaat in de top-k
            var lijst = waarde_map[a];
            double bovenste = lijst[r].Item2; 
            drempel += bovenste;
            max[a] = bovenste;

            int tid = lijst[r].Item1;
            double score = 0;

            // Als we een nieuwe TID vinden, bereken we de score van de gehele tupel
            if (!gezien.Contains(tid))
            {
                gezien.Add(tid);
                foreach (var a2 in tid_map.Keys)
                    score += tid_map[a2][tid].Item2;

                buffer.Add(tid, score);
            }
        }

        List<int> te_verwijderen = new List<int>();

        foreach (var kandidaat in buffer.Keys)
        {
            if (buffer[kandidaat] > drempel) 
            {
                topK.Add(Tuple.Create(kandidaat, buffer[kandidaat]));
                //if (topK.Count == k) break;
                te_verwijderen.Add(kandidaat);
            }
        }

        foreach(var s in te_verwijderen)
            buffer.Remove(s);


        r++;
    }

    topK.Sort((x, y) => y.Item2.CompareTo(x.Item2));

    Tuple<int, double>[] res = new Tuple<int,double>[k];
    for(int i = 0; i < k; i++)
        res[i] = topK[i];

    return res;
}

public static DataView vul_topk(Tuple<int, double>[] top)
{
    StringBuilder sb = new StringBuilder();
    sb.Append("SELECT * FROM autompg WHERE ");

    for(int i = 0; i < top.Length; i++)
    {
        string s = (i < top.Length-1) ? " OR " : ";";
        sb.Append(" id = " + top[i].Item1 + s);
    }
    SQLiteConnection sc = new SQLiteConnection(verbinding_tekst);
    sc.Open();

    SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(sb.ToString(), verbinding_tekst);
    DataSet dset = new DataSet();
    int ia = myAdapter.Fill(dset);

    sc.Close();

    DataTable dt = dset.Tables[0];

    dt.Columns.Add("score", typeof(double));

    foreach (DataRow dr in dt.Rows)
        for(int i = 0; i < top.Length; i++)
            if (dr["id"].ToString() == top[i].Item1.ToString())
                dr["score"] = top[i].Item2;    

    return dset.Tables[0].DefaultView;
}

private static double get_IDF(string attr, string waarde)
{
    return Autobank.get_scalar(String.Format("SELECT idf FROM Bijzonderheid WHERE attribuut = '{0}' AND waarde = '{1}'", attr, waarde));
}

private static double get_H(string attr)
{
    return Autobank.get_scalar(String.Format("SELECT bandbreedte FROM Dichtheid WHERE attribuut = '{0}'", attr));
}

private static double get_QF(string attr, string waarde)
{
    return Autobank.get_scalar(String.Format("SELECT qf FROM Frequentie WHERE attribuut = '{0}' AND waarde = '{1}'", attr, waarde));
}

private static double get_Jac(string attr, string term1, string term2, bool onveilig = true)
{
    return Autobank.get_scalar(String.Format("SELECT jaccard FROM Similarity WHERE attribuut = '{0}' AND term1 = '{1}' AND term2 = '{1}'", attr, term1, term2), onveilig);
}
}
}