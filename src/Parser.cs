using System;
using System.Collections.Generic;
using System.IO;

namespace Practicum1 {

public static class Parser
{

public static Dictionary<string, string> parse_query(string q)
{
    string[] invoer = q.Split(',');
    int n = invoer.Length;
    Dictionary<string,string> C = new Dictionary<string, string>();

    for (int i = 0; i < n; i++)
    {
        string[] s = invoer[i].Split('=');
        C[s[0].Trim()] = s[1].Trim(" ';".ToCharArray());
    }

    return C;
}


public static void parse_workload(string bestandsnaam, Dictionary<string, Dictionary<string, int>> map, Dictionary<string, Dictionary<Tuple<string, string>, int>> in_map)
{
    StreamReader stream = new StreamReader("../../data/workload.txt");

    string regel = stream.ReadLine();
    int n = int.Parse(regel.Split(' ')[0]);
    regel = stream.ReadLine();

    while (true)
    {
        regel = regel = stream.ReadLine();
        if (regel == "") break;

        int qf = int.Parse(regel.Split(new char[] { ' ' }, 2)[0]);

        string where_clause = regel.Split(new string[] { "WHERE" }, StringSplitOptions.None)[1];
        string[] voorwaarden = where_clause.Split(new string[] { "AND" }, StringSplitOptions.None);

        foreach(var v in voorwaarden)
        {
            if (v.Contains("=")) // attribuut = waarde
            {
                string[] xs = v.Split(new char[] { '=' }, 2);

                var attr   = xs[0].Trim();
                var waarde = xs[1].Trim(" '".ToCharArray());

                if (!map.ContainsKey(attr))
                    map[attr] = new Dictionary<string, int>();
                
                if (map[attr].ContainsKey(waarde))
                    map[attr][waarde] += qf;
                else map[attr][waarde] = qf;
            }
            else // IN clausule
            {
                string[] xs = v.Split(new string[] { "IN" }, StringSplitOptions.None);
                string attr = xs[0].Trim();
                string[] waarden = xs[1].Trim(" ()'".ToCharArray()).Replace("\'", "").Split(',');

                if (!in_map.ContainsKey(attr))
                    in_map[attr] = new Dictionary<Tuple<string, string>, int>();

                foreach(string w in waarden)
                {
                    voeg_toe(map[attr], w, qf);

                    foreach(string t in waarden)
                        voeg_toe(in_map[attr], Tuple.Create(w, t), qf);
                }
            }
        }
    }
}

public static void voeg_toe<T>(IDictionary<T, int> map, T key, int v)
{
    if (map.ContainsKey(key))
         map[key] += v;
    else map[key]  = v;
}

}
}
