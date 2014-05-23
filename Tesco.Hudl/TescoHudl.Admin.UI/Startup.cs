using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(TescoHudl.Admin.UI.Startup))]
namespace TescoHudl.Admin.UI
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
