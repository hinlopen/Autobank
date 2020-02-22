using System;
using System.Windows;
using System.Windows.Input;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;

namespace DAR1 {
public partial class MainWindow : Window {

public MainWindow()
{
    InitializeComponent();
    System.Threading.Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.CreateSpecificCulture("en-GB");
    //resultaten.Columns.Add(new DataGridColumn ("score") { DataType = typeof(double) });

    Autobank.begin();
    log("Voer een zoekopdracht in");

    {
        Dictionary<string, string> condities = Parser.parse_zoekopdracht("cylinders = 4"); // cylinders = 4, brand='ford', type='convertible', k=3;");

        int k = 10;
        if (condities.ContainsKey("k"))
        {
            k = int.Parse(condities["k"]);
            condities.Remove("k");
        }

        var top_k = Zoekmachine.VerwerkQuery(condities, k);
        resultaten.ItemsSource = Zoekmachine.vul_topk(top_k);
    }
    
    zoek_knop.KeyDown += new KeyEventHandler(zoek_met_enter);
}

private void zoek_knop_klik (object sender, RoutedEventArgs e)
{
    resultaten.ItemsSource = null;
    resultaten.Items.Refresh();
    // try 
    // {
        Dictionary<string,string> condities = Parser.parse_zoekopdracht(invoer_box.Text.ToLower());

        int k = 10;
        if (condities.ContainsKey("k"))
        {
            k = int.Parse(condities["k"]);
            condities.Remove("k");
        }

        var top_k = Zoekmachine.VerwerkQuery(condities, k);
        resultaten.ItemsSource = Zoekmachine.vul_topk(top_k);
    // }
    // catch (Exception)
    // {
    //     MessageBox.Show("Ongeldige zoekopdracht", "Foutmelding", MessageBoxButton.OK, MessageBoxImage.Error);
    // }
}


private void zoek_met_enter(object sender, KeyEventArgs e)
{
    if (e.Key == Key.Enter)
        zoek_knop_klik(sender, e);
}

private void log(string s)
{
    meldingen.Content = s;
}

private void log()
{
    log("");
}

}
}
