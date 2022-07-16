namespace Bygdrift.Warehouse.Helpers.Attributes
{
    /// <summary>
    /// How to handle if a setting or secret are not set
    /// </summary>
    public enum NotSet
    {
        /// <summary>No action</summary>
        DoNothing,
        /// <summary>An information will be send by log and can be seen in Apllication Insights</summary>
        ShowLogInfo,
        /// <summary>An error will be send by log and can be seen in Apllication Insights</summary>
        ShowLogError,
        /// <summary>A warning will be send by log and can be seen in Apllication Insights</summary>
        ShowLogWarning,
        /// <summary>The system will stop excecution if this setting is missing</summary>
        ThrowError,
    }
}
