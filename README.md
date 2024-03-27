Add Project in Solution First
After That add reference to layer that includes startup.cs
Add project as assembly and add controller to main application
         var assembly = Assembly.Load("Dashboard");
         services.AddMvc().AddApplicationPart(assembly).AddControllersAsServices();

After adding run the server
Note : - All the API will be under the name of Dashboard
