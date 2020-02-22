using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.IO;

namespace DAR1
{
public static class Autobank
{

static string[] attributen  = new string[] {"mpg", "cylinders", "displacement", "horsepower", "weight", "acceleration", "model_year", "origin", "brand", "model", "type" };
static string[] numerisch   = new string[] {"mpg", "displacement", "horsepower", "weight", "acceleration", "model_year" };
static string[] categorisch = new string[] {"cylinders", "origin", "brand", "model", "type" };

static string res_pad = "../../../res/";
static string db_naam = res_pad + "autobank.sqlite";
static string verbinding_tekst = "Data Source=" + db_naam + ";Version=3;";
static int N;

public static double get_scalar(string sql, bool onveilig = false)
{
    double res = 0;
    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();
        SQLiteCommand cmd = new SQLiteCommand(sql, sc);
        SQLiteDataReader reader;
        reader = cmd.ExecuteReader();
        if (reader.Read())
            res = double.Parse(reader.GetDouble(0).ToString());
        else
            res = 1;

    }

    return res;
}

public static int tel_tupels(string R, string where_clause = "")
{
    int aantal = 0;
    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();
        SQLiteCommand cmd = new SQLiteCommand("SELECT COUNT(*) FROM " + R + " " + where_clause, sc);
        aantal = int.Parse(cmd.ExecuteScalar().ToString());
        sc.Close();
    } 

    return aantal;
}

public static List<Tuple<string, double>> vergelijkbare_attributen(string a, string w)
{
    var similarities = new List<Tuple<string, double>>();
    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();
        SQLiteCommand command = new SQLiteCommand(String.Format("SELECT term2, jaccard FROM Similarity WHERE attribuut = '{0}' AND term1 = '{1}'", a, w), sc);
        SQLiteDataReader T = command.ExecuteReader();
        while (T.Read())
        {
            var v = Tuple.Create(T["term2"].ToString(), double.Parse(T["jaccard"].ToString()));
            similarities.Add(v);
        }

        sc.Close();
    }

    similarities.Sort((x,y) => y.Item2.CompareTo(x.Item2)); //todo, idf

    return similarities;
}

public static List<string> populairste_attributen(List<string> attributen)
{
    List<string> gesorteerd = new List<string>(attributen.Count);

    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();
        SQLiteCommand command = new SQLiteCommand("SELECT * FROM FrequentieA ORDER BY rqf DESC", sc);
        SQLiteDataReader T = command.ExecuteReader();
        while (T.Read())
        {
            string a = T["attribuut"].ToString() ;
            if (attributen.Contains(a))
                gesorteerd.Add(a);
        }
        sc.Close();
    }
    return gesorteerd;
}

public static void begin()
{
    bool vanaf_begin = true;
    bool alleen_meta = false;
    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        if (System.IO.File.Exists(db_naam))
        {
            sc.Open();
            
            SQLiteCommand cmd = new SQLiteCommand("SELECT name FROM sqlite_master WHERE type='table' AND name='Dichtheid'", sc);
            SQLiteDataReader T = cmd.ExecuteReader();

            while (T.Read())
            {
                vanaf_begin = "Dichtheid" != T[0].ToString();
            }

            if (!vanaf_begin)
            {
                //alleen_meta = tel_tupels("Dichtheid") < 1;
            }

            sc.Close();
        }
        else
        {
            SQLiteConnection.CreateFile(db_naam);
        }
    }

    if (vanaf_begin)
    {
        uitvoeren_query_buffer("autompg.sql");
        uitvoeren_query_buffer("meta.sql");
    }
    if (vanaf_begin || alleen_meta)
    {
        maak_meta_database();
        uitvoeren_query_buffer("metaload.txt");  
    }
    N = tel_tupels("autompg");
}

public static void maak_meta_database() 
{
    N = tel_tupels("autompg");

    Dictionary<string, int>[] frequenties = new Dictionary<string, int>[categorisch.Length];
    for (int i = 0; i < categorisch.Length; i++) 
        frequenties[i] = new Dictionary<string, int>();

    double[,] getallen = new double[numerisch.Length, N]; //voor elk attr. de voorkomede getallen
    double[]  totalen  = new double[numerisch.Length]; //totaal voor elk attribuut, voor standaardafwijking;

    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();

        var cmd = new SQLiteCommand("SELECT mpg, displacement, horsepower, weight, acceleration, model_year FROM autompg", sc);
        SQLiteDataReader Tupel = cmd.ExecuteReader();
        int k = 0;
        while (Tupel.Read())
        {
            for (int i = 0; i < numerisch.Length; i++)
            {
                double x = double.Parse(Tupel[numerisch[i]].ToString());
                totalen[i] += x;
                getallen[i, k] = x;
            }
            k++;
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

    StreamWriter bestand = new StreamWriter(@"..\..\..\res\metaload.txt");

    double[] H = new double[numerisch.Length];
    for (int a = 0; a < numerisch.Length; a++)
    {
        double gem = totalen[a] / (double)N;
        double sd  = 0;
        for (int i = 0; i < N; i++)
        {
            double x = getallen[a,i] - gem;
            sd += x*x;
        }
        sd   = Math.Sqrt(sd / (double)(N-1));
        H[a] = 1.06 * Math.Pow(sd, -0.2);

        bestand.WriteLine("INSERT INTO Dichtheid VALUES ('{0}', '{1}');", numerisch[a], H[a]);
    }

    for (int a = 0; a < numerisch.Length; a++)
    {
        double ih, e;
        e  = Math.E;
        ih = 1.0 / H[a];
        HashSet<double> bekend = new HashSet<double>();

        for (int j = 0; j < N; j++)
        {
            double idf = 0;
            double t = getallen[a, j];
            if (bekend.Contains(t)) continue;
            else                    bekend.Add(t);

            for (int i = 0; i < N; i++)
            {
                double contributie = (getallen[a, i] - t) * ih;
                idf += Math.Pow(e, -0.5 * (contributie*contributie));
            }

            idf = Math.Log10(N / idf);

            bestand.WriteLine("INSERT INTO Bijzonderheid VALUES ('{0}', '{1}', '{2}');", numerisch[a], t, idf);
        }
    }

    for (int a = 0; a < categorisch.Length; a++)
    {
        var fs = frequenties[a];
        foreach(var f in fs.Keys) 
        {
            double idf = Math.Log10(N / (double)fs[f]);
            bestand.WriteLine("INSERT INTO Bijzonderheid VALUES ('{0}', '{1}', '{2}');", categorisch[a], f, idf);
        }
    }

    //
    // QF en value similarity
    //
    
    var map = new Dictionary<string, Dictionary<string, int>>();
    var in_map = new Dictionary<string, Dictionary<Tuple<string, string>, int>>();
    Parser.parse_workload("workload.txt", map, in_map);

    var irqf_max = new Dictionary<string, double>();

    foreach (string a in map.Keys)
    {
        irqf_max[a] = 1.0 / (map[a].Values.Max()+1);
        bestand.WriteLine("INSERT INTO FrequentieA VALUES ('{0}', '{1}'); ", a, map[a].Sum(x => x.Value));
    }


    foreach(string a in map.Keys)
        foreach(string k in map[a].Keys)
            bestand.WriteLine("INSERT INTO Frequentie VALUES ('{0}', '{1}', '{2}'); ", a, k, (map[a][k]+1) * irqf_max[a]);

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
            double qf = map[a][w] * irqf_max[a];
            double S = jaccard * qf;

            bestand.WriteLine("INSERT INTO Similarity VALUES ('{0}', '{1}', '{2}', '{3}'); ", a, t, w, S);
        }

    bestand.Close();
    bestand.Dispose();
}

public static void uitvoeren_query_buffer(string bestandsnaam)
{
    string[] regels = System.IO.File.ReadAllText(res_pad + bestandsnaam).Split(';');
    int j = 0;
    using(SQLiteConnection sc = new SQLiteConnection(verbinding_tekst))
    {
        sc.Open();

        using (var transactie = sc.BeginTransaction())
        using (var cmd = new SQLiteCommand(sc))
        {
            while (j < regels.Length)
            {
                var sb = new StringBuilder(regels[j].Length);

                for(int i = 0; i < regels[j].Length; i++)
                {
                    char s = regels[j][i];
                    if (s != '\n' && s != '\r')
                        sb.Append(s);
                }
                
                sb.Append(';');
                cmd.CommandText = sb.ToString();
                cmd.ExecuteNonQuery();
                j++;
            }
            
            transactie.Commit();
        }

        sc.Close();
    }
}

}
}
