1. Add Project in Solution First

2. After That add reference to layer that includes startup.cs

3. Add project as assembly and add controller to main application
         var assembly = Assembly.Load("Dashboard");
         services.AddMvc().AddApplicationPart(assembly).AddControllersAsServices();

4. After adding run the server
   
Note : - All the API will be under the name of Dashboard
