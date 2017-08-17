using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SQLServerToJSON {
    class Program {
        static void Main(string[] args) {
            (new Extractor()).Run();
        }
    }
}
