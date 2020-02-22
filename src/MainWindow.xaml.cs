using System;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Input;

namespace Practicum1 {

public partial class MainWindow : Window
{

Autobank db;

public MainWindow()
{
    InitializeComponent();

    System.Threading.Thread.CurrentThread.CurrentCulture = System.Globalization.CultureInfo.CreateSpecificCulture("en-GB");

    db = new Autobank();
    string s1 = "k = 20, brand = 'ford', cylinders = 6, weight=3092;";
    string s2 = "brand='nissan'";
    string s3 = "k= 50, brand='mercury', type='sedan'";
    string s4 = "k=20, brand='mercedes-benz'";
    string s5 = "weight = 6000";

    zoek_resultaten.ItemsSource = db.verwerk_zoekopdracht(s2);
    zoek_veld.Text = s2;
}

private void zoek_knop_klik(object sender, RoutedEventArgs e)
{
    zoek_resultaten.ItemsSource = null;
    zoek_resultaten.Items.Clear();
    zoek_resultaten.ItemsSource = db.verwerk_zoekopdracht(zoek_veld.Text.ToLower());
}

}
}
