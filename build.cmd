
MSBuild.exe azuresblite.sln /p:Configuration=Release

IF NOT EXIST ".\Build\Packages" MKDIR ".\Build\Packages"

.\Tools\NuGet\NuGet.exe pack AzureSBLite.nuspec -OutputDirectory ".\Build\Packages"
