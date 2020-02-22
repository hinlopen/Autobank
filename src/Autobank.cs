using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.IO;
using System.Text;

namespace Practicum1
{

struct DB_Info
{
    public string naam;
    public string pad;
    public string verbinding_tekst;
}

public class Autobank
{

DB_Info db;

string[] A = new string[] {"mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type" };
string[] numeriek   = new string[] {"mpg", "displacement", "horsepower", "weight", "acceleration", "model_year" };
string[] categorisch = new string[] {"cylinders", "origin", "brand", "model", "type" };

string[] zoek_attributen;     // De attributen die voorkomen in de zoekopdracht
List<string> zoek_attributen_num;
List<string> zoek_attributen_cat;

int N, m;
Dictionary<string, string> C;
Dictionary<string, double> oorspronkelijk; // Onafgeronde termen
Dictionary<string, double> H_idf_inverse, H_qf_inverse;
Dictionary<string, double> IDF, QF;
Dictionary<string, Dictionary<string, double>> Jac;
Dictionary<string, List<Tuple<int,double>>> attr_scores;
const double e = Math.E;

Dictionary<string, Tuple<int,int,int>> bins; // Numerieke waarden worden afgerond voordat ze beoordeeld worden.

public Autobank()
{
    db.naam = "autompg.sqlite";
    db.pad = "../../data/";
    db.verbinding_tekst = "Data Source=" + db.pad + db.naam + ";Version=3;";

    init_bins();

    bool vanaf_begin = !File.Exists(db.pad + db.naam);

    if (vanaf_begin)
    {
        bulk_query_uitvoeren("autompg.sql");
        bulk_query_uitvoeren("meta.sql");

        if (!File.Exists(db.pad + "metaload.txt"))
            vul_meta_tabellen();
        bulk_query_uitvoeren("metaload.txt");
    }

    N = tel_tupels("autompg");
}

private void init_bins()
{
    bins = new Dictionary<string, Tuple<int,int,int>>(); // oorsprong, einde, stapgrootte
    bins["mpg"]          = Tuple.Create(10,50,2);
    bins["displacement"] = Tuple.Create(50,500,25);
    bins["horsepower"]   = Tuple.Create(40,330,10);
    bins["weight"]       = Tuple.Create(2000,5000,200);
    bins["acceleration"] = Tuple.Create(10,30,2);
    bins["model_year"]   = Tuple.Create(65,85,1);
}

// Hoofdfunctie
public DataView verwerk_zoekopdracht(string q)
{
    C = Parser.parse_query(q);

    // Filter k uit de zoektermen
    int k = 10;
    if (C.ContainsKey("k"))
    {
        k = int.Parse(C["k"]);
        C.Remove("k");
    }
    m = C.Count;

    // Deel zoektermen op in categorische en numerieke attributen.
    verdeel_zoektermen();
    
    oorspronkelijk = new Dictionary<string, double>();
    rond_numerieke_attributen_af();
    
    // Voorbereiden tabellen (idf, qf, etc.)
    init_tabellen();
   
    // Converteer tupels naar scorelijsten voor attributen.
    bereken_scores();
   
    // Selecteer de k beste tupels
    Tuple<int,double>[] resultaten = top_k(k);

    // Als er tupels zijn met dezelfde score, geef deze extra score voor missende attributen
    resultaten = eventuele_gelijke_scores_oplossen(resultaten);
   
    // Return een tabel met complete tupels.
    return tids_naar_dataview(resultaten);
}

private void rond_numerieke_attributen_af()
{
    foreach(string a in zoek_attributen_num)
    {
        if (!bins.ContainsKey(a)) continue;

        double v = double.Parse(C[a]);

        oorspronkelijk[a] = v;
        C[a] = rond_getal_af(v, a).ToString();
    }
}

private int rond_getal_af(double v, string attr)
{
    var b = bins[attr];

    if      (v < b.Item1) v = b.Item1;
    else if (v > b.Item2) v = b.Item2;
    else
    {
        double rest = v % b.Item3;
        v += (rest < b.Item3 / 2) ? -rest : (b.Item3 - rest);
    }

    return (int)v;
}

private void verdeel_zoektermen()
{
    int i = 0;
    zoek_attributen = new string[m];
    zoek_attributen_cat = new List<string>();
    zoek_attributen_num = new List<string>();
    foreach(string a in C.Keys)
    {
        zoek_attributen[i] = a;
        if (categorisch.Contains(a)) zoek_attributen_cat.Add(a);
        else                         zoek_attributen_num.Add(a);
        i++;
    }
}

private void init_tabellen()
{
    H_idf_inverse = new Dictionary<string, double>(m);
    H_qf_inverse = new Dictionary<string, double>(m);
    IDF = new Dictionary<string, double>(m);
    QF  = new Dictionary<string, double>(m);
    Jac = new Dictionary<string, Dictionary<string, double>>(m);
    
    string basis = "SELECT {0} FROM {1} WHERE attribuut = '{2}' AND waarde = '{3}'";
    string basis2 = "SELECT {0} FROM {1} WHERE attribuut = '{2}'";
    foreach(string a in zoek_attributen_cat)
    {
        IDF[a] = get_scalar(String.Format(basis, "idf", "IDF", a, C[a]));
        QF[a] = get_scalar(String.Format(basis, "qf", "QF", a, C[a]));

        Jac[a] = get_jaccard_scores(a, C[a]);
    }

    foreach(string a in zoek_attributen_num)
    {
        H_idf_inverse[a] = 1.0 / get_scalar(String.Format(basis2, "bandbreedte", "Hidf", a));
        H_qf_inverse[a]  = 1.0 / get_scalar(String.Format(basis2, "bandbreedte", "Hqf", a));
        IDF[a] = get_scalar(String.Format(basis, "idf", "IDF", a, C[a]));
    }
}

public void bereken_scores()
{
    // Maak kolommen aan voor elk attribuut
    attr_scores = new Dictionary<string, List<Tuple<int, double>>>(m);
    foreach(string a in zoek_attributen)
        attr_scores[a] = new List<Tuple<int, double>>(N);
    
    using (SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst))
    {
        sc.Open();
        SQLiteCommand command = new SQLiteCommand("SELECT * FROM autompg ORDER BY id;", sc);
        SQLiteDataReader Tupel = command.ExecuteReader();
        int i = 1; 

        // Haal tupels uit de autompg database ga stapsgewijs door elke kolom
        while(Tupel.Read())
        {
            foreach (string a in zoek_attributen_cat)
            {
                double score = 0;
                string t = Tupel[a].ToString();
                if (t == C[a])
                {
                    score = IDF[a];
                }
                else if (Jac.ContainsKey(a)) // Alleen automerk en -type hebben een Jaccard score
                {
                    if (Jac[a].ContainsKey(t))
                        score = Jac[a][t] * IDF[a];
                }

                if (QF.ContainsKey(a))
                    score *= QF[a];
                
                attr_scores[a].Add(Tuple.Create(i, score));
            }

            foreach(string a in zoek_attributen_num)
            {
                double t = double.Parse(Tupel[a].ToString());
                double q = oorspronkelijk[a];
                
                double d = (t - q) * H_idf_inverse[a];
                double score = Math.Pow(e, -0.5 * (d * d)) * IDF[a];
                if (QF.ContainsKey(a))
                {
                    d = (t - q) * H_qf_inverse[a];
                    score *= Math.Pow(e, -0.5 * (d * d)) * QF[a];
                }
                
                attr_scores[a].Add(Tuple.Create(i, score));
            }
            i++;
        }
        sc.Close();
    }
}

public Tuple<int,double>[] top_k(int k)
{
    // Kopieer scores naar een nieuwe dictionary van lijsten, die we sorteren
    var gesorteerd = new Dictionary<string, List<Tuple<int, double>>>();
    foreach(string a in zoek_attributen) {
        var b = new Tuple<int, double>[N];
        attr_scores[a].CopyTo(b);
        gesorteerd[a] = b.ToList();
        gesorteerd[a].Sort((x, y) => y.Item2.CompareTo(x.Item2));
    }

    Dictionary<int, double> buffer = new Dictionary<int, double>();
    Dictionary<string, double> max = new Dictionary<string, double>(gesorteerd.Count);
    List<Tuple<int, double>> top   = new List<Tuple<int, double>>(k);
    double drempel;

    HashSet<int> gezien = new HashSet<int>();
    int r = 0;

    while (top.Count < k && r < N) {
        drempel = 0;
        // Elke ronde elke attribuut een stap verder
        foreach(var a in zoek_attributen) {
            // De maximale score (som van attr.) die nog behaald kan worden
            // Alles wat we vinden dat hierboven zit, gaat in de top-k
            var lijst = gesorteerd[a];
            double bovenste = lijst[r].Item2; 
            max[a] = bovenste;
            drempel += bovenste;

            int tid = lijst[r].Item1;

            // Als we een nieuwe tupel vinden, bereken we de score van de gehele tupel
            if (!gezien.Contains(tid)) {
                gezien.Add(tid);
                double score = 0;
                foreach (var b in zoek_attributen)
                    score += attr_scores[b][tid-1].Item2; // Lijst[0] heeft tupel id = 1

                buffer.Add(tid, score);
            }
        }

        List<int> te_verwijderen = new List<int>();

        foreach (var kandidaat in buffer.Keys) {
            if (buffer[kandidaat] >= drempel) {
                top.Add(Tuple.Create(kandidaat, buffer[kandidaat]));
                te_verwijderen.Add(kandidaat);
            }
        }

        foreach(var s in te_verwijderen)
            buffer.Remove(s);

        r++;
    }

    top.Sort((x, y) => y.Item2.CompareTo(x.Item2));

    Tuple<int, double>[] res = new Tuple<int,double>[k];
    for(int i = 0; i < k; i++)
       res[i] = top[i];

    return res;
}

public Tuple<int, double>[] eventuele_gelijke_scores_oplossen(Tuple<int, double>[] top)
{
    List<int> unieke_tupels = new List<int>();

    // Dubbele scores detecteren.
    // Dubbele waardes kunnen alleen direct na elkaar voorkomen, dus kan met eenmalig door de lijst heen gaan. 
    int i = 0;
    while (i < top.Length)
    {
        double val = top[i].Item2;
        unieke_tupels.Add(i);

        do {
            i++;
        }
        while(i < top.Length && top[i].Item2 == val);
    }

    // Alle waarde uniek, dan is het al prima
    if (unieke_tupels.Count == top.Length) return top;

    // Maak equivalentie klassen
    List<List<int>> klassen = new List<List<int>>();
    int l = 0;
    for (i = 0; i < unieke_tupels.Count; i++)
    {
        int volgende;
        if (i == unieke_tupels.Count-1) volgende = top.Length-i+1;
        else                            volgende = unieke_tupels[i+1];

        if (volgende - i < 2) continue;
        
        klassen.Add(new List<int>());
        int start = unieke_tupels[i];
        for (int j = start; j < volgende; j++)
        {
            klassen[l].Add(j);
        }
        l++;
    }

    // Stel missende attributen op
    List<string> missende_attributen = new List<string>(A);

    missende_attributen.Remove("model_year");
    missende_attributen.Remove("origin");
    missende_attributen.Remove("model");

    foreach(string a in zoek_attributen)
        missende_attributen.Remove(a);

    // Bereken de extra score binnen elke klasse door som van IDF * epsilon
    foreach (var klas in klassen)
    {
        foreach (int k in klas)
        {
            int tid = top[k].Item1;
            double oude_score = top[k].Item2;
            double extra_score = 0;
            var tupel = new Dictionary<string,string>(missende_attributen.Count);

            // Sla alle nodige attribuutwaarden van één tupel op
            using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst))
            {
                sc.Open();

                var command = new SQLiteCommand("SELECT * FROM autompg WHERE id = " + tid, sc);
                var Tupel = command.ExecuteReader();
                while(Tupel.Read())
                {
                    foreach(string a in missende_attributen)
                    {
                        tupel[a] = Tupel[a].ToString();
                    }                    
                }

                sc.Close();
            }

            // Tel de totale extra score op
            foreach (string a in missende_attributen)
            {
                string t = tupel[a];
                if (numeriek.Contains(a))
                    t = rond_getal_af(double.Parse(tupel[a]), a).ToString();
    
                string sql = String.Format("SELECT {0} FROM {1} WHERE attribuut = '{2}' AND waarde = '{3}'", "idf", "IDF", a, t);

                extra_score += get_scalar(sql) * 0.0005;
            }

            top[k] = Tuple.Create(tid, oude_score+extra_score);
        }
    }

    // Sorteer opnieuw
    var top_lijst = top.ToList();
    top_lijst.Sort((x, y) => y.Item2.CompareTo(x.Item2));

    return top_lijst.ToArray();
}

public DataView tids_naar_dataview(Tuple<int, double>[] top)
{
    StringBuilder sb = new StringBuilder();
    sb.Append("SELECT * FROM autompg WHERE ");

    for(int i = 0; i < top.Length; i++)
    {
        string s = (i < top.Length-1) ? " OR " : ";";
        sb.Append(" id = " + top[i].Item1 + s);
    }

    DataSet dset = new DataSet();
    using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst)) {
        sc.Open();

        SQLiteDataAdapter myAdapter = new SQLiteDataAdapter(sb.ToString(), db.verbinding_tekst);
        int ia = myAdapter.Fill(dset);

        sc.Close();
    }

    DataTable dt = dset.Tables[0];

    var kolom = dt.Columns.Add("score", typeof(double));
    kolom.SetOrdinal(0);

    foreach (DataRow dr in dt.Rows)
        for(int i = 0; i < top.Length; i++)
            if (dr["id"].ToString() == top[i].Item1.ToString())
                dr["score"] = top[i].Item2.ToString("0.0000");

    dt.DefaultView.Sort = "score desc";

    return dset.Tables[0].DefaultView;
}

double bereken_numeriek_idf (string attr, double term, double h_inverse)
{
    var termen = new double[N];

    using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst)) {
        sc.Open();

        var command = new SQLiteCommand("SELECT " + attr + " FROM autompg", sc);
        var Tupel = command.ExecuteReader();
        int i = 0; 
        while(Tupel.Read())
            termen[i++] = double.Parse(Tupel[attr].ToString());

        sc.Close();
    }

    double idf = 0;
    for (int i = 0; i < N; i++)
    {
        double x = (termen[i] - term) * h_inverse;
        idf += Math.Pow(e, -0.5 * (x*x));
    }

    return Math.Log10(N / idf);;
}

public double get_scalar(string sql)
{
    double res = 0;
    using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst)) {
        sc.Open();
        SQLiteCommand cmd = new SQLiteCommand(sql, sc);
        SQLiteDataReader reader;
        reader = cmd.ExecuteReader();
        if (reader.Read())
            res = double.Parse(reader.GetDouble(0).ToString());
        else
            res = 0; // @Todo, wat hier? 1 of 0?
    }

    return res;
}

private Dictionary<string,double> get_jaccard_scores(string attr, string term1)
{
    var res = new Dictionary<string, double>();
        using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst)) {
        sc.Open();

        SQLiteCommand cmd = new SQLiteCommand(String.Format("SELECT * FROM Jaccard WHERE attribuut = '{0}' AND term1 = '{1}'", attr, term1), sc);
        var Tupel = cmd.ExecuteReader();
        while (Tupel.Read())
        {
            res[Tupel["term2"].ToString()] = double.Parse(Tupel["jaccard"].ToString());
        }
    }
    return res;
}

public int tel_tupels(string R, string where_clause = "")
{
    int aantal = 0;
    using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst))
    {
        sc.Open();
        SQLiteCommand cmd = new SQLiteCommand("SELECT COUNT(*) FROM " + R + " " + where_clause, sc);
        aantal = int.Parse(cmd.ExecuteScalar().ToString());
        sc.Close();
    } 

    return aantal;
}

#region meta database constructie code
public void bulk_query_uitvoeren(string bestandsnaam)
{
    string[] regels = System.IO.File.ReadAllText(db.pad + bestandsnaam).Split(';');
    int j = 0;
    using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst))
    {
        sc.Open();

        using (var transactie = sc.BeginTransaction())
        using (var cmd = new SQLiteCommand(sc))
        {
            while (j < regels.Length)
            {
                string regel = regels[j++];
                cmd.CommandText = regel.Trim(new char[] {'\r', '\n', ' '});
                cmd.ExecuteNonQuery();
            }
            
            transactie.Commit();
        }

        sc.Close();
    }
}

private void vul_meta_tabellen()
{
    N = tel_tupels("autompg");

    Dictionary<string, int>[] frequenties = new Dictionary<string, int>[categorisch.Length];
    for (int i = 0; i < categorisch.Length; i++) 
        frequenties[i] = new Dictionary<string, int>();

    double[,] getallen = new double[numeriek.Length, N]; // De gesorteerd voor elk attribuut
    double[]  totalen  = new double[numeriek.Length];    // Totaal voor elk attribuut, voor standaardafwijking;

    using(SQLiteConnection sc = new SQLiteConnection(db.verbinding_tekst))
    {
        sc.Open();

        var cmd = new SQLiteCommand("SELECT mpg, displacement, horsepower, weight, acceleration, model_year FROM autompg", sc);
        var Tupel = cmd.ExecuteReader();
        int j = 0;
        while (Tupel.Read())
        {
            for (int i = 0; i < numeriek.Length; i++)
            {
                double x = double.Parse(Tupel[numeriek[i]].ToString());
                totalen[i] += x;
                getallen[i, j] = x;
            }
            j++;
        }
        
        cmd = new SQLiteCommand("SELECT cylinders, origin, brand, model, type FROM autompg", sc);
        Tupel = cmd.ExecuteReader();
        while (Tupel.Read())
        {
            for (int i = 0; i < categorisch.Length; i++)
            {
                string w = Tupel[categorisch[i]].ToString();
                if (frequenties[i].ContainsKey(w)) frequenties[i][w]++;
                else                               frequenties[i].Add(w, 1);
            }
        }

        sc.Close();
    }

    StreamWriter bestand = new StreamWriter(db.pad + "metaload.txt");

#region IDF
    // IDF voor categorische waarden
    for (int a = 0; a < categorisch.Length; a++)
    {
        var fs = frequenties[a];
        foreach(var f in fs.Keys) 
        {
            double idf = Math.Log10(N / (double)fs[f]);
            bestand.WriteLine("INSERT INTO IDF VALUES ('{0}', '{1}', '{2}');", categorisch[a], f, idf);
        }
    }


    // IDF voor numerieke waarden
    double[] H_inverse = new double[numeriek.Length];
    for (int a = 0; a < numeriek.Length; a++)
    {
        double gem = totalen[a] / (double)N;
        double sd  = 0;
        for (int i = 0; i < N; i++)
        {
            double x = getallen[a,i] - gem;
            sd += x*x;
        }
        sd   = Math.Sqrt(sd / (double)(N-1));
        double h = 1.06 * sd * Math.Pow(N, -0.2);
        bestand.WriteLine("INSERT INTO Hidf VALUES ('{0}', '{1}');", numeriek[a], h);

        H_inverse[a] = 1.0 / h; 
    }

    for (int j = 0; j < numeriek.Length; j++)
    {
        string attribuut = numeriek[j];
        double h_inverse = H_inverse[j];
        var b = bins[attribuut];

        for (int t = b.Item1; t <= b.Item2; t += b.Item3)
        {
            double idf = 0;

            for (int i = 0; i < N; i++)
            {
                double contributie = (getallen[j, i] - t) * h_inverse;
                idf += Math.Pow(e, -0.5 * (contributie*contributie));
            }

            idf = Math.Log10(N / idf);

            bestand.WriteLine("INSERT INTO IDF VALUES ('{0}', '{1}', '{2}');", attribuut, t, idf);
        }            
    }


#endregion

#region QF

    //Parse workload bestand
    var map = new Dictionary<string, Dictionary<string, int>>();
    var in_map = new Dictionary<string, Dictionary<Tuple<string, string>, int>>();
    Parser.parse_workload("workload.txt", map, in_map);

    var irqf_max = new Dictionary<string, double>(map.Keys.Count);
    var rqf_max  = new Dictionary<string, double>(map.Keys.Count);

    // Voorberkeningen
    foreach (string a in map.Keys)
    {
        rqf_max[a]  = map[a].Values.Max();
        irqf_max[a] = 1.0 / (rqf_max[a]+1);

        bestand.WriteLine("INSERT INTO FrequentieA VALUES ('{0}', '{1}'); ", a, map[a].Sum(x => x.Value));
    }

    // QF voor categorische waarden  
    foreach(string a in categorisch)
    {
        if (!map.ContainsKey(a)) continue;
        foreach(string k in map[a].Keys)
            bestand.WriteLine("INSERT INTO QF VALUES ('{0}', '{1}', '{2}'); ", a, k, (map[a][k]+1) * irqf_max[a]);
    }

    //QF voor numerieke waarden
    //Hqf

    for (int i = 0; i < H_inverse.Length; i++)
        H_inverse[i] = 0;

    for (int j = 0; j < numeriek.Length; j++)
    {
        string attribuut = numeriek[j];
        if (!map.ContainsKey(attribuut)) continue;
        var L = map[attribuut];
        int n = L.Values.Sum(x => x);
        double gem = L.Sum(x => double.Parse(x.Key) * x.Value) / (double)n;
        double som = 0;
        foreach(int t in L.Values)
        {
            double x = t - gem;
            som += x*x;
        }
        double sd = Math.Sqrt(som / (double)(n-1));
        double h = 1.06 * sd * Math.Pow(n, -0.2);
        bestand.WriteLine("INSERT INTO Hqf VALUES ('{0}', '{1}');", attribuut, h);

        H_inverse[j] = 1.0 / h; // @Fixme, mogelijk conflicterend met eerdere h waarde voor idf
    }

    // (R)QF 

    var rqfs = new Dictionary<string, Dictionary<int, double>>();

    for (int j = 0; j < numeriek.Length; j++)
    {
        string attribuut = numeriek[j];
        if (!map.ContainsKey(attribuut)) continue;

        double h_inverse = H_inverse[j];
        var b = bins[attribuut];
        rqfs[attribuut] = new Dictionary<int, double>();
        double max_tot_nu = 0;

        for (int t = b.Item1; t <= b.Item2; t += b.Item3)
        {
            double rqf = 0;

            foreach(string q in map[attribuut].Keys)
            {
                double x = (double.Parse(q) - t) * h_inverse;
                double getal = Math.Pow(e, -0.5 * (x*x));       
                rqf += getal * map[attribuut][q]; // Afstand van getal q tot t, herhaald omdat q vaker voorkwam
            }

            rqfs[attribuut][t] = rqf;
            if (rqf > max_tot_nu) max_tot_nu = rqf;
        }

        foreach(int t in rqfs[attribuut].Keys)
        {
            double qf = (rqfs[attribuut][t]+1) / (max_tot_nu+1);
            bestand.WriteLine("INSERT INTO QF VALUES ('{0}', '{1}', '{2}');", attribuut, t, qf);
        }
    }
#endregion QF

    foreach(string a in in_map.Keys)
        foreach(var k in in_map[a])
        {
            string t, w;
            int tf, wf, vereniging, doorsnede;

            t = k.Key.Item1;
            w = k.Key.Item2;

            tf = in_map[a][Tuple.Create(t, t)];
            wf = in_map[a][Tuple.Create(w, w)];
            vereniging = tf + wf - k.Value;
            doorsnede  = k.Value;
            double jaccard = doorsnede / (double)vereniging;
            double S = jaccard;

            bestand.WriteLine("INSERT INTO Jaccard VALUES ('{0}', '{1}', '{2}', '{3}'); ", a, t, w, S);
        }

    bestand.Close();
    bestand.Dispose();
}

#endregion

}
}
