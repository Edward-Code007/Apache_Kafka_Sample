public class ResultPattern<T>
{
    public T? Value;
    public bool isSuccess;
    public string Error = "";
   

    public static ResultPattern<T> Success(T value)
    {
        return new ResultPattern<T>()
        {
            isSuccess = true,
            Error = "",
            Value = value
        };
    }
    public static ResultPattern<T> Failed(string err)
    {
        return new ResultPattern<T>()
        {
          Error = err,
          Value = default, 
          isSuccess = false 
        };
    }

    public static implicit operator ResultPattern<T>(T value) => Success(value);
    public static implicit operator ResultPattern<T>(string err) => Failed(err);
}