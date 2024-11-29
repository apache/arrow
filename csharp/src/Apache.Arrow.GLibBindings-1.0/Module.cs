namespace Apache.Arrow.GLibBindings;

public static class Module
{
    public static void Initialize()
    {
        if (_isInitialized)
        {
            return;
        }

        GObject.Module.Initialize();

        Internal.ImportResolver.RegisterAsDllImportResolver();
        Internal.TypeRegistration.RegisterTypes();

        _isInitialized = true;
    }

    private static bool _isInitialized;
}
