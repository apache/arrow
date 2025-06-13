namespace Apache.Arrow.Flight.Middleware.Interfaces;

public interface IFlightClientMiddlewareFactory
{
    IFlightClientMiddleware OnCallStarted(CallInfo callInfo);
}